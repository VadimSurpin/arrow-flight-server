package pro.surpin.data.arrowflight.client.spark;

import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Or;
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.plans.Cross$;
import org.apache.spark.sql.catalyst.plans.Inner$;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.View;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.PartialFunction;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractPartialFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Reads a table once for several scalar aggregates over it that are cross joined: {@code (SELECT agg1 FROM t WHERE p1)
 * x CROSS JOIN (SELECT agg2 FROM t WHERE p2) y} becomes one aggregate over one scan whose filter is {@code p1 OR p2},
 * and each aggregate function is limited to the rows of its own branch with {@code FILTER (WHERE p)}.
 *
 * <p>The branches may join tables, as TPC-DS q88 does with four of them eight times over: branches that join the same
 * tables with the same conditions and differ in what they filter share their scans, each table's scan taking the
 * {@code OR} of the filters its branches put on it, and each branch's aggregates keep the filters that told the
 * branches apart after the join. A scalar aggregate yields one row, so the cross join of such branches has one row and
 * the merged aggregate yields it under the same output attributes.
 *
 * <p>Branches with a distinct aggregate are left alone: gating several of them makes Spark expand every row once per
 * distinct group, which costs more than the scans it saves (TPC-DS q28 on Spark: 24 s instead of 10 s).
 *
 * <p>Uncorrelated scalar subqueries in a projection are merged during resolution, before their scans are pushed down.
 * Cross-joined aggregate branches are merged during operator optimization. Both rewrites are off unless
 * {@value #CONF} is true.
 */
public final class FlightScanSharing extends Rule<LogicalPlan> {
    /** Session setting that turns the rule on. */
    public static final String CONF = "spark.flight.scanSharing";

    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScanSharing.class);

    private final Predicate<DataSourceV2Relation> shareable;

    /** The rule for the tables of this connector. */
    public FlightScanSharing() {
        this(relation -> relation.table() instanceof FlightTable);
    }

    /**
     * Builds the rule for the relations a predicate accepts.
     *
     * @param shareable tells whether a scan of the relation is worth sharing
     */
    FlightScanSharing(Predicate<DataSourceV2Relation> shareable) {
        this.shareable = shareable;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (!Boolean.parseBoolean(SQLConf.get().getConfString(CONF, "false"))) {
            return plan;
        }
        return rewrite(plan);
    }

    private LogicalPlan rewrite(LogicalPlan plan) {
        if (plan instanceof Project project) {
            return shareSubqueries(project).mapChildren(new AbstractFunction1<LogicalPlan, LogicalPlan>() {
                @Override
                public LogicalPlan apply(LogicalPlan child) {
                    return rewrite(child);
                }
            });
        }
        if (!isCross(plan)) {
            return plan.mapChildren(new AbstractFunction1<LogicalPlan, LogicalPlan>() {
                @Override
                public LogicalPlan apply(LogicalPlan child) {
                    return rewrite(child);
                }
            });
        }
        List<LogicalPlan> leaves = new ArrayList<>();
        collectLeaves(plan, leaves);
        boolean changed = false;
        for (int i = 0; i < leaves.size(); i++) {
            LogicalPlan rewritten = rewrite(leaves.get(i));
            changed |= rewritten != leaves.get(i);
            leaves.set(i, rewritten);
        }
        List<LogicalPlan> merged = share(leaves);
        if (merged.size() == leaves.size() && !changed) {
            return plan;
        }
        LogicalPlan chain = merged.get(0);
        for (int i = 1; i < merged.size(); i++) {
            chain = new Join(chain, merged.get(i), Inner$.MODULE$, Option.empty(), JoinHint.NONE());
        }
        return chain;
    }

    /** Replaces compatible scalar subqueries in a projection with columns from one shared aggregate. */
    private LogicalPlan shareSubqueries(Project project) {
        List<ScalarSubquery> subqueries = new ArrayList<>();
        for (NamedExpression named : list(project.projectList())) {
            collectSubqueries((Expression) named, subqueries);
        }
        Map<String, List<ScalarSubquery>> byShape = new LinkedHashMap<>();
        Map<ExprId, Branch> branches = new HashMap<>();
        for (ScalarSubquery subquery : subqueries) {
            if (!subquery.outerAttrs().isEmpty() || !subquery.joinCond().isEmpty()) {
                continue;
            }
            Branch branch = branchOf(subquery.plan());
            if (branch != null && branch.aggregate.aggregateExpressions().size() == 1) {
                branches.put(subquery.exprId(), branch);
                byShape.computeIfAbsent(branch.shape, k -> new ArrayList<>()).add(subquery);
            }
        }
        LogicalPlan child = project.child();
        Map<ExprId, Expression> replacements = new HashMap<>();
        for (List<ScalarSubquery> group : byShape.values()) {
            if (group.size() < 2) {
                continue;
            }
            List<Branch> members = new ArrayList<>();
            group.forEach(subquery -> members.add(branches.get(subquery.exprId())));
            LogicalPlan merged = merge(members);
            if (merged == null) {
                continue;
            }
            List<Attribute> output = list(merged.output());
            for (int i = 0; i < group.size(); i++) {
                replacements.put(group.get(i).exprId(), output.get(i));
            }
            child = new Join(child, merged, Cross$.MODULE$, Option.empty(), JoinHint.NONE());
            LOGGER.info("scan sharing: merged {} scalar subqueries over {}", group.size(), members.get(0).shape);
        }
        if (replacements.isEmpty()) {
            return project;
        }
        List<NamedExpression> expressions = new ArrayList<>();
        for (NamedExpression named : list(project.projectList())) {
            expressions.add((NamedExpression) replaceSubqueries((Expression) named, replacements));
        }
        return new Project(seq(expressions), child);
    }

    /** Collects scalar subqueries from an expression without entering their plans. */
    private static void collectSubqueries(Expression expression, List<ScalarSubquery> out) {
        if (expression instanceof ScalarSubquery subquery) {
            out.add(subquery);
            return;
        }
        for (Expression child : list(expression.children())) {
            collectSubqueries(child, out);
        }
    }

    /** Replaces merged scalar subqueries by the aggregate output attributes. */
    private static Expression replaceSubqueries(Expression expression, Map<ExprId, Expression> replacements) {
        PartialFunction<Expression, Expression> swap = new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Expression node) {
                return node instanceof ScalarSubquery subquery && replacements.containsKey(subquery.exprId());
            }

            @Override
            @SuppressWarnings("unchecked")
            public <A1 extends Expression, B1> B1 applyOrElse(A1 node, Function1<A1, B1> otherwise) {
                if (isDefinedAt(node)) {
                    return (B1) replacements.get(((ScalarSubquery) node).exprId());
                }
                return otherwise.apply(node);
            }
        };
        return expression.transform(swap);
    }

    private static boolean isCross(LogicalPlan plan) {
        return plan instanceof Join join && (join.joinType() instanceof Inner$ || join.joinType() instanceof Cross$)
                && join.condition().isEmpty()
                && join.hint().equals(JoinHint.NONE());
    }

    private static void collectLeaves(LogicalPlan plan, List<LogicalPlan> leaves) {
        if (isCross(plan)) {
            Join join = (Join) plan;
            collectLeaves(join.left(), leaves);
            collectLeaves(join.right(), leaves);
        } else {
            leaves.add(plan);
        }
    }

    /** One scan of a branch and the conjuncts of the filter right above it. */
    private static final class Scan {
        private final DataSourceV2Relation relation;
        private final List<Expression> filters = new ArrayList<>();

        private Scan(DataSourceV2Relation relation) {
            this.relation = relation;
        }
    }

    /** One scalar aggregate over a tree of inner joins of filtered scans. */
    private static final class Branch {
        private final Aggregate aggregate;
        // scans left to right, join conditions with children before parents
        private final List<Scan> scans = new ArrayList<>();
        private final List<Expression> conditions = new ArrayList<>();
        private String shape;
        private String why;

        private Branch(Aggregate aggregate) {
            this.aggregate = aggregate;
        }
    }

    private Branch branchOf(LogicalPlan leaf) {
        if (!(leaf instanceof Aggregate aggregate) || !aggregate.groupingExpressions().isEmpty()) {
            return null;
        }
        for (NamedExpression named : list(aggregate.aggregateExpressions())) {
            Expression expression = (Expression) named;
            if (!expression.deterministic() || exists(expression, e -> e instanceof AggregateExpression a && a.isDistinct())
                    || !exists(expression, e -> e instanceof AggregateExpression)) {
                return null;
            }
        }
        Branch branch = new Branch(aggregate);
        branch.shape = flatten(aggregate.child(), branch);
        if (branch.shape == null) {
            LOGGER.info("scan sharing: aggregate not shared: {}", branch.why);
        }
        return branch.shape == null ? null : branch;
    }

    /**
     * Describes the tables and joins under {@code plan}, null unless these are inner joins with a condition of scans
     * that may be shared; notes scans, conditions and filters in {@code branch}. Branches can be merged only if they
     * describe the same.
     */
    private String flatten(LogicalPlan plan, Branch branch) {
        if (plan instanceof SubqueryAlias alias) {
            return flatten(alias.child(), branch);
        }
        if (plan instanceof View view) {
            return flatten(view.child(), branch);
        }
        if (plan instanceof Project project) {
            for (NamedExpression column : list(project.projectList())) {
                if (!(column instanceof Attribute)) {
                    branch.why = "projection computes " + column;
                    return null;
                }
            }
            return flatten(project.child(), branch);
        }
        if (plan instanceof Filter filter) {
            LogicalPlan below = withoutProjects(filter.child());
            if (below instanceof DataSourceV2Relation relation && shareable.test(relation)) {
                Scan scan = new Scan(relation);
                conjuncts(filter.condition(), scan.filters);
                branch.scans.add(scan);
                return relation.table().name();
            }
            branch.why = "filter over " + below.nodeName();
            return null;
        }
        if (plan instanceof Join join) {
            if (!(join.joinType() instanceof Inner$) || join.condition().isEmpty()) {
                branch.why = "join " + join.joinType() + " on " + join.condition();
                return null;
            }
            String left = flatten(join.left(), branch);
            String right = left == null ? null : flatten(join.right(), branch);
            if (right == null) {
                return null;
            }
            branch.conditions.add(join.condition().get());
            return "(" + left + " " + join.hint() + " " + right + ")";
        }
        if (plan instanceof DataSourceV2Relation relation && shareable.test(relation)) {
            branch.scans.add(new Scan(relation));
            return relation.table().name();
        }
        branch.why = "operator " + plan.nodeName();
        return null;
    }

    private static LogicalPlan withoutProjects(LogicalPlan plan) {
        while (plan instanceof Project || plan instanceof SubqueryAlias || plan instanceof View) {
            if (plan instanceof Project project) {
                plan = project.child();
            } else if (plan instanceof SubqueryAlias alias) {
                plan = alias.child();
            } else {
                plan = ((View) plan).child();
            }
        }
        return plan;
    }

    /** Merges the branches that join the same tables into the first of them; returns the leaves that remain, in order. */
    private List<LogicalPlan> share(List<LogicalPlan> leaves) {
        Map<String, List<Integer>> byShape = new LinkedHashMap<>();
        Map<Integer, Branch> branches = new HashMap<>();
        for (int i = 0; i < leaves.size(); i++) {
            Branch branch = branchOf(leaves.get(i));
            if (branch != null) {
                branches.put(i, branch);
                byShape.computeIfAbsent(branch.shape, k -> new ArrayList<>()).add(i);
            }
        }
        Map<Integer, LogicalPlan> replaced = new HashMap<>();
        Set<Integer> dropped = new HashSet<>();
        for (Map.Entry<String, List<Integer>> group : byShape.entrySet()) {
            if (group.getValue().size() < 2) {
                continue;
            }
            List<Branch> members = new ArrayList<>();
            group.getValue().forEach(index -> members.add(branches.get(index)));
            LogicalPlan merged = merge(members);
            LOGGER.info("scan sharing: {} aggregates over {}: {}", members.size(), group.getKey(),
                    merged == null ? "not merged" : "merged");
            if (merged != null) {
                replaced.put(group.getValue().get(0), merged);
                dropped.addAll(group.getValue().subList(1, group.getValue().size()));
            }
        }
        List<LogicalPlan> result = new ArrayList<>();
        for (int i = 0; i < leaves.size(); i++) {
            if (!dropped.contains(i)) {
                result.add(replaced.getOrDefault(i, leaves.get(i)));
            }
        }
        return result;
    }

    private LogicalPlan merge(List<Branch> members) {
        Branch first = members.get(0);
        int tables = first.scans.size();

        // one attribute per column of each scanned table, the first branch's wherever it has one
        List<Map<String, AttributeReference>> unified = new ArrayList<>();
        for (int table = 0; table < tables; table++) {
            Map<String, AttributeReference> columns = new LinkedHashMap<>();
            for (Branch branch : members) {
                for (AttributeReference attribute : list(branch.scans.get(table).relation.output())) {
                    columns.putIfAbsent(attribute.name(), attribute);
                }
            }
            unified.add(columns);
        }
        List<Map<ExprId, Expression>> toUnified = new ArrayList<>();
        for (Branch branch : members) {
            Map<ExprId, Expression> map = new HashMap<>();
            for (int table = 0; table < tables; table++) {
                for (AttributeReference attribute : list(branch.scans.get(table).relation.output())) {
                    map.put(attribute.exprId(), unified.get(table).get(attribute.name()));
                }
            }
            toUnified.add(map);
        }

        // the joins must be the same, whatever the branches filter
        List<Expression> conditions = new ArrayList<>();
        for (int join = 0; join < first.conditions.size(); join++) {
            Expression condition = replace(first.conditions.get(join), toUnified.get(0));
            for (int branch = 1; branch < members.size(); branch++) {
                Expression other = replace(members.get(branch).conditions.get(join), toUnified.get(branch));
                if (!condition.semanticEquals(other)) {
                    LOGGER.info("scan sharing: joins not shared: {} is {} in one branch and {} in another", join,
                            condition, other);
                    return null;
                }
            }
            conditions.add(condition);
        }

        // per table, what every branch filters alike stays on the scan; the rest is told apart after the join
        List<List<List<Expression>>> filters = new ArrayList<>();
        for (int branch = 0; branch < members.size(); branch++) {
            List<List<Expression>> perTable = new ArrayList<>();
            for (Scan scan : members.get(branch).scans) {
                List<Expression> mapped = new ArrayList<>();
                final Map<ExprId, Expression> map = toUnified.get(branch);
                scan.filters.forEach(filter -> mapped.add(replace(filter, map)));
                perTable.add(mapped);
            }
            filters.add(perTable);
        }
        List<Expression> scanFilters = new ArrayList<>();
        List<List<Expression>> own = new ArrayList<>();
        members.forEach(branch -> own.add(new ArrayList<>()));
        for (int table = 0; table < tables; table++) {
            List<Expression> common = new ArrayList<>();
            for (Expression conjunct : filters.get(0).get(table)) {
                boolean everywhere = true;
                for (List<List<Expression>> perTable : filters) {
                    everywhere &= contains(perTable.get(table), conjunct);
                }
                if (everywhere && !contains(common, conjunct)) {
                    common.add(conjunct);
                }
            }
            List<Expression> alternatives = new ArrayList<>();
            boolean open = false;
            for (int branch = 0; branch < members.size(); branch++) {
                List<Expression> rest = new ArrayList<>();
                for (Expression conjunct : filters.get(branch).get(table)) {
                    if (!contains(common, conjunct) && !contains(rest, conjunct)) {
                        rest.add(conjunct);
                    }
                }
                own.get(branch).addAll(rest);
                if (rest.isEmpty()) {
                    open = true;
                } else if (!containsAll(alternatives, rest)) {
                    alternatives.add(conjunction(rest));
                }
            }
            List<Expression> parts = new ArrayList<>(common);
            if (!open) {
                parts.add(disjunction(alternatives));
            }
            scanFilters.add(parts.isEmpty() ? null : conjunction(parts));
        }

        List<NamedExpression> aggregates = new ArrayList<>();
        for (int branch = 0; branch < members.size(); branch++) {
            Expression gate = own.get(branch).isEmpty() ? null : conjunction(own.get(branch));
            for (NamedExpression expression : list(members.get(branch).aggregate.aggregateExpressions())) {
                Expression mapped = replace((Expression) expression, toUnified.get(branch));
                aggregates.add((NamedExpression) (gate == null ? mapped : gated(mapped, gate)));
            }
        }

        List<LogicalPlan> scans = new ArrayList<>();
        for (int table = 0; table < tables; table++) {
            DataSourceV2Relation relation = first.scans.get(table).relation;
            DataSourceV2Relation shared = relation.copy(relation.table(), seq(new ArrayList<AttributeReference>(unified.get(table).values())),
                    relation.catalog(), relation.identifier(), relation.options());
            scans.add(scanFilters.get(table) == null ? shared : new Filter(scanFilters.get(table), shared));
        }
        LogicalPlan joined = rebuild(first.aggregate.child(), scans.iterator(), conditions.iterator(), first);
        return new Aggregate(seq(new ArrayList<Expression>()), seq(aggregates), joined);
    }

    /** The joins of {@code plan} over the shared scans, without the projections in between. */
    private LogicalPlan rebuild(LogicalPlan plan, java.util.Iterator<LogicalPlan> scans,
                                java.util.Iterator<Expression> conditions, Branch first) {
        if (plan instanceof Project project) {
            return rebuild(project.child(), scans, conditions, first);
        }
        if (plan instanceof Join join) {
            LogicalPlan left = rebuild(join.left(), scans, conditions, first);
            LogicalPlan right = rebuild(join.right(), scans, conditions, first);
            return new Join(left, right, join.joinType(), Option.apply(conditions.next()), join.hint());
        }
        return scans.next();
    }

    /** {@code expression} with every aggregate function limited to the rows that pass {@code gate}. */
    private static Expression gated(Expression expression, Expression gate) {
        PartialFunction<Expression, Expression> limit = new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Expression node) {
                return node instanceof AggregateExpression;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <A1 extends Expression, B1> B1 applyOrElse(A1 node, Function1<A1, B1> otherwise) {
                if (node instanceof AggregateExpression a) {
                    Expression condition = a.filter().isDefined() ? new And(a.filter().get(), gate) : gate;
                    return (B1) new AggregateExpression(a.aggregateFunction(), a.mode(), a.isDistinct(),
                            Option.apply(condition), a.resultId());
                }
                return otherwise.apply(node);
            }
        };
        return expression.transform(limit);
    }

    /** {@code expression} with the attributes in {@code map} replaced. */
    private static Expression replace(Expression expression, Map<ExprId, Expression> map) {
        PartialFunction<Expression, Expression> swap = new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Expression node) {
                return node instanceof AttributeReference a && map.containsKey(a.exprId());
            }

            @Override
            @SuppressWarnings("unchecked")
            public <A1 extends Expression, B1> B1 applyOrElse(A1 node, Function1<A1, B1> otherwise) {
                if (isDefinedAt(node)) {
                    return (B1) map.get(((AttributeReference) node).exprId());
                }
                return otherwise.apply(node);
            }
        };
        return expression.transform(swap);
    }

    private static boolean exists(Expression expression, Predicate<Expression> test) {
        return (Boolean) expression.exists(new AbstractFunction1<Expression, Object>() {
            @Override
            public Object apply(Expression node) {
                return test.test(node);
            }
        });
    }

    private static void conjuncts(Expression expression, List<Expression> out) {
        if (expression instanceof And and) {
            conjuncts(and.left(), out);
            conjuncts(and.right(), out);
        } else {
            out.add(expression);
        }
    }

    private static boolean contains(List<Expression> expressions, Expression expression) {
        return expressions.stream().anyMatch(e -> e.semanticEquals(expression));
    }

    /** Whether an alternative is the conjunction of exactly these conjuncts (an alternative is built from a list). */
    private static boolean containsAll(List<Expression> alternatives, List<Expression> conjuncts) {
        return contains(alternatives, conjunction(conjuncts));
    }

    private static Expression conjunction(List<Expression> parts) {
        Expression result = parts.get(0);
        for (int i = 1; i < parts.size(); i++) {
            result = new And(result, parts.get(i));
        }
        return result;
    }

    private static Expression disjunction(List<Expression> parts) {
        Expression result = parts.get(0);
        for (int i = 1; i < parts.size(); i++) {
            result = new Or(result, parts.get(i));
        }
        return result;
    }

    private static <T> scala.collection.immutable.List<T> seq(List<T> list) {
        return JavaConverters.asScalaBuffer(list).toList();
    }

    private static <T> List<T> list(scala.collection.Seq<T> seq) {
        return new ArrayList<>(JavaConverters.seqAsJavaListConverter(seq).asJava());
    }
}
