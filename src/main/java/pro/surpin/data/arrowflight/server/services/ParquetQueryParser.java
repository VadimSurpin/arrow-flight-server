package pro.surpin.data.arrowflight.server.services;

import org.jooq.*;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses SQL SELECT queries against Parquet tables, extracting schema, table, columns, filters, and join info.
 */
public class ParquetQueryParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetQueryParser.class);
    private static final int PARSE_CACHE_CAPACITY = 1024;
    private static final Map<String, ParquetQueryParser> PARSE_CACHE =
            Collections.synchronizedMap(
                    new LinkedHashMap<>(PARSE_CACHE_CAPACITY, 0.75f, true));

    /**
     * A table reference in a JOIN, with optional schema and alias.
     */
    public record JoinTable(String schema, String table, String alias) {}

    /**
     * A selected expression in a query, describing its aggregation function and column references.
     */
    public static final class SelectExpr {
        /**
         * Supported aggregation function types.
         */
        public enum AggFunc { COLUMN, COUNT_STAR, COUNT, SUM, MIN, MAX }

        public final AggFunc func;
        public final String inputColumn;
        public final String inputExpression;
        public final List<String> inputColumns;
        public final Integer decimalScale;
        public final String outputName;

        /**
         * @param func aggregation function type
         * @param inputColumn raw column expression text
         * @param outputName rendered column alias
         */
        SelectExpr(AggFunc func, String inputColumn, String outputName) {
            this(func, inputColumn, inputColumn,
                    inputColumn == null ? Collections.emptyList() : List.of(inputColumn),
                    null, outputName);
        }

        /**
         * Creates a parsed select expression with its executable SQL and physical inputs.
         *
         * @param func aggregation function type
         * @param inputColumn unquoted input text used by simple-column paths
         * @param inputExpression quoted SQL expression used by DuckDB
         * @param inputColumns physical columns referenced by the expression
         * @param decimalScale decimal input scale, or null for non-decimal inputs
         * @param outputName rendered column alias
         */
        SelectExpr(AggFunc func, String inputColumn, String inputExpression,
                List<String> inputColumns, Integer decimalScale, String outputName) {
            this.func = func;
            this.inputColumn = inputColumn;
            this.inputExpression = inputExpression;
            this.inputColumns = List.copyOf(inputColumns);
            this.decimalScale = decimalScale;
            this.outputName = outputName;
        }
    }

    public final String schema;
    public final String table;
    public final List<String> columns;
    public final String filter;

    public final boolean hasAggregation;
    public final List<String> groupByColumnNames;
    public final List<SelectExpr> selectExprs;

    public final boolean isJoin;
    public final List<JoinTable> joinTables;
    public final String duckDbSql;

    /**
     * @param schema table schema name
     * @param table table name
     * @param columns selected column names
     * @param filter WHERE clause expression
     * @param hasAggregation whether query uses aggregation
     * @param groupByColumnNames GROUP BY column names
     * @param selectExprs parsed select expressions
     */
    private ParquetQueryParser(String schema, String table, List<String> columns, String filter,
            boolean hasAggregation, List<String> groupByColumnNames, List<SelectExpr> selectExprs) {
        this(schema, table, columns, filter, hasAggregation, groupByColumnNames, selectExprs,
                false, Collections.emptyList(), null);
    }

    /**
     * @param schema table schema name
     * @param table primary table name
     * @param columns selected column names
     * @param filter WHERE clause expression
     * @param hasAggregation whether query uses aggregation
     * @param groupByColumnNames GROUP BY column names
     * @param selectExprs parsed select expressions
     * @param isJoin whether query involves JOINs
     * @param joinTables list of joined tables
     * @param duckDbSql rewritten SQL for DuckDB execution
     */
    private ParquetQueryParser(String schema, String table, List<String> columns, String filter,
            boolean hasAggregation, List<String> groupByColumnNames, List<SelectExpr> selectExprs,
            boolean isJoin, List<JoinTable> joinTables, String duckDbSql) {
        this.schema = schema;
        this.table = table;
        this.columns = List.copyOf(columns);
        this.filter = filter;
        this.hasAggregation = hasAggregation;
        this.groupByColumnNames = List.copyOf(groupByColumnNames);
        this.selectExprs = List.copyOf(selectExprs);
        this.isJoin = isJoin;
        this.joinTables = List.copyOf(joinTables);
        this.duckDbSql = duckDbSql;
    }

    /**
     * @param query SQL SELECT query string
     * @return parsed query representation
     * @throws IllegalArgumentException if query is not a SELECT
     * @throws RuntimeException if parsing fails
     */
    public static ParquetQueryParser parse(String query) {
        // Flatten subquery wrappers before jOOQ parsing (regex is more robust)
        String flattened = flattenSubqueryWrapper(query);
        if (!flattened.equals(query)) {
            LOGGER.debug("Flattened subquery wrapper; parsing: {}", flattened);
            query = flattened;
        }
        ParquetQueryParser cached = PARSE_CACHE.get(query);
        if (cached != null) {
            return cached;
        }

        Settings settings = new Settings()
                .withRenderFormatted(true)
                .withParseNameCase(ParseNameCase.AS_IS);
        DSLContext ctx = DSL.using(SQLDialect.DEFAULT, settings);

        try {
            Query parsedQuery = ctx.parser().parseQuery(query);

            if (parsedQuery instanceof Select<?> select) {
                LOGGER.debug("Parsing SQL query: {}", query);
                ParquetQueryParser parser = parseSelect(select, query);
                synchronized (PARSE_CACHE) {
                    PARSE_CACHE.put(query, parser);
                    if (PARSE_CACHE.size() > PARSE_CACHE_CAPACITY) {
                        PARSE_CACHE.remove(PARSE_CACHE.keySet().iterator().next());
                    }
                }
                LOGGER.debug("Parsed query: {}", parser);
                return parser;
            } else {
                throw new IllegalArgumentException("Only SELECT queries are supported: " + query);
            }

        } catch (Exception e) {
            LOGGER.error("Error parsing SQL query", e);
            throw new RuntimeException("Invalid SQL query: " + query, e);
        }
    }

    /**
     * Flattens "SELECT ... FROM (SELECT ...) alias" → "SELECT ... FROM innerTable".
     * Regex-based, operates on the raw SQL string before jOOQ parsing.
     *
     * @param query raw SQL query string
     * @return flattened query, or original if no subquery wrapper detected
     */
    @SuppressWarnings("java:S3776")
    private static String flattenSubqueryWrapper(String query) {
        String q = query.trim();

        Matcher fromParen = Pattern.compile("(?i)\\bfrom\\s*\\(").matcher(q);
        if (!fromParen.find()) {
            return q;
        }

        int openParen = fromParen.end() - 1;
        String beforeFrom = q.substring(0, fromParen.start());

        int depth = 1;
        int i = openParen + 1;
        while (i < q.length() && depth > 0) {
            char c = q.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            i++;
        }
        if (depth != 0) {
            return q;
        }

        String innerSQL = q.substring(openParen + 1, i - 1).trim();
        String afterParen = q.substring(i).trim();

        Matcher aliasM = Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)(.*)$",
                Pattern.DOTALL).matcher(afterParen);
        if (!aliasM.matches()) {
            return q;
        }
        String outerRest = aliasM.group(2).trim();

        if (!innerSQL.toLowerCase(Locale.ROOT).startsWith("select")) {
            return q;
        }

        Matcher innerFrom = Pattern.compile("(?i)\\bfrom\\s+(\\S+)").matcher(innerSQL);
        if (!innerFrom.find()) {
            return q;
        }
        String realTableRef = innerFrom.group(1).replaceAll("\\s*$", "");

        String innerWhere = "";
        Matcher innerWhereM = Pattern.compile("(?i)\\bwhere\\b(.+)$",
                Pattern.DOTALL).matcher(innerSQL);
        if (innerWhereM.find()) {
            innerWhere = innerWhereM.group(1).trim();
        }

        StringBuilder flat = new StringBuilder(beforeFrom);
        flat.append("from ").append(realTableRef);

        String outerWhere = "";
        if (outerRest.toLowerCase(Locale.ROOT).startsWith("where ")) {
            outerWhere = outerRest.substring(5).trim();
        } else if (!outerRest.isEmpty()) {
            return q;
        }

        boolean hasInner = !innerWhere.isEmpty();
        boolean hasOuter = !outerWhere.isEmpty();
        if (hasInner && hasOuter) {
            flat.append(" where (").append(innerWhere).append(") and (").append(outerWhere).append(")");
        } else if (hasInner) {
            flat.append(" where ").append(innerWhere);
        } else if (hasOuter) {
            flat.append(" where ").append(outerWhere);
        }

        return flat.toString();
    }

    /**
     * @param select jOOQ Select query object
     * @param originalSql original SQL string for fallback parsing
     * @return parsed query representation
     */
    private static ParquetQueryParser parseSelect(Select<?> select, String originalSql) {
        List<? extends Table<?>> fromTables = select.$from();

        DSLContext noQuoteCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.NEVER));

        if (fromTables.size() > 1) {
            throw new IllegalArgumentException(
                    "Can only select from ONE table, but got query: " + select.getSQL());
        }

        // Detect JOIN via jOOQ rendered output
        boolean isJoin = hasJoinKeywords(select);

        if (isJoin) {
            return parseJoinSelect(select, noQuoteCtx, originalSql);
        }
        return parseSingleTableSelect(select, fromTables, noQuoteCtx);
    }

    /**
     * Detects JOIN by checking whether the (single) FROM table renders with "join" keyword.
     * jOOQ rendering is deterministic for all JOIN types.
     *
     * @param select jOOQ Select query object
     * @return true if query contains JOIN
     */
    private static boolean hasJoinKeywords(Select<?> select) {
        List<? extends Table<?>> from = select.$from();
        if (from.size() > 1) {
            return true;
        }
        if (from.size() == 1) {
            String rendered = DSL.using(SQLDialect.DEFAULT)
                    .render(from.get(0)).toLowerCase(Locale.ROOT);
            return rendered.contains(" join ");
        }
        return false;
    }

    /**
     * Extracts table references from a JOIN SQL string using regex.
     * Only matches table references immediately after FROM/JOIN keywords,
     * avoiding false matches in ON clause column references.
     */
    private static final Pattern JOIN_KEYWORD = Pattern.compile(
            "\\b(?:FROM|(?:(?:LEFT|RIGHT|FULL(?:\\s+OUTER)?|INNER|CROSS)\\s+)?JOIN)\\s+",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern TABLE_REF = Pattern.compile(
            "^(\\w+(?:\\.\\w+)?)(?:\\s+(?:AS\\s+)?(\\w+))?",
            Pattern.CASE_INSENSITIVE);

    /**
     * @param sql SQL query string
     * @return list of extracted join tables
     */
    private static List<JoinTable> extractJoinTables(String sql) {
        List<JoinTable> tables = new ArrayList<>();
        Matcher kw = JOIN_KEYWORD.matcher(sql);
        while (kw.find()) {
            String rest = sql.substring(kw.end()).trim();
            Matcher tr = TABLE_REF.matcher(rest);
            if (tr.find()) {
                String qualified = tr.group(1);
                String alias = tr.group(2);
                int dot = qualified.indexOf('.');
                String schema = dot > 0 ? qualified.substring(0, dot) : null;
                String table = dot > 0 ? qualified.substring(dot + 1) : qualified;
                if (alias == null) {
                    alias = table;
                }
                tables.add(new JoinTable(schema, table, alias));
            }
        }
        return tables;
    }

    /**
     * @param select jOOQ Select query object
     * @param noQuoteCtx DSL context with quoted names disabled
     * @param originalSql original SQL string for regex-based join extraction
     * @return parsed query representation with join info
     */
    private static ParquetQueryParser parseJoinSelect(Select<?> select,
            DSLContext noQuoteCtx, String originalSql) {

        List<JoinTable> joinTables = extractJoinTables(originalSql);
        if (joinTables.isEmpty()) {
            throw new IllegalArgumentException("Could not extract table references from JOIN: " + originalSql);
        }

        List<String> columns = new ArrayList<>();
        List<SelectExpr> exprs = new ArrayList<>();
        for (SelectFieldOrAsterisk sfoa : select.$select()) {
            if (!(sfoa instanceof org.jooq.Field<?>)) {
                continue;
            }
            org.jooq.Field<?> f = (org.jooq.Field<?>) sfoa;
            columns.add(f.getName());
            String outputName = noQuoteCtx.renderInlined(f).trim();
            exprs.add(new SelectExpr(SelectExpr.AggFunc.COLUMN, outputName, outputName));
        }

        Condition where = select.$where();
        DSLContext quotedCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));
        String filter = quotedCtx.renderInlined(where);

        String duckDbSql = rewriteTableRefs(originalSql, joinTables);

        JoinTable primary = joinTables.get(0);
        return new ParquetQueryParser(primary.schema, primary.table, columns, filter,
                false, Collections.emptyList(), exprs, true, joinTables, duckDbSql);
    }

    /**
     * @param select jOOQ Select query object
     * @param fromTables list of FROM tables from the query
     * @param noQuoteCtx DSL context with quoted names disabled
     * @return parsed query representation for a single-table query
     */
    @SuppressWarnings("java:S3776")
    private static ParquetQueryParser parseSingleTableSelect(Select<?> select,
            List<? extends Table<?>> fromTables, DSLContext noQuoteCtx) {

        if (fromTables.size() != 1) {
            throw new IllegalArgumentException("Can only select from ONE table, but got query: " + select.getSQL());
        }

        String schema = Optional.ofNullable(fromTables.get(0).getSchema()).map(Schema::getName).orElse(null);
        String table = fromTables.get(0).getName();

        List<String> gbCols = new ArrayList<>();
        for (GroupField gf : select.$groupBy()) {
            gbCols.add(noQuoteCtx.render(gf).trim());
        }

        List<String> columns = new ArrayList<>();
        List<SelectExpr> exprs = new ArrayList<>();
        boolean hasAgg = !gbCols.isEmpty();
        DSLContext quotedCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));

        for (SelectFieldOrAsterisk sfoa : select.$select()) {
            if (!(sfoa instanceof org.jooq.Field<?>)) {
                continue;
            }
            org.jooq.Field<?> selected = (org.jooq.Field<?>) sfoa;
            org.jooq.Field<?> f = selected;
            String outputName = noQuoteCtx.renderInlined(selected).trim();
            if (selected instanceof org.jooq.impl.QOM.FieldAlias<?> alias) {
                f = alias.$field();
                outputName = selected.getName();
            }
            columns.add(selected.getName());

            if (f instanceof org.jooq.impl.QOM.Count count) {
                hasAgg = true;
                org.jooq.Field<?> arg = count.$field();
                if (arg == null || arg instanceof org.jooq.Param<?>) {
                    exprs.add(new SelectExpr(SelectExpr.AggFunc.COUNT_STAR, null, outputName));
                } else {
                    exprs.add(aggregateExpression(SelectExpr.AggFunc.COUNT,
                            arg, outputName, noQuoteCtx, quotedCtx));
                }
            } else if (f instanceof org.jooq.impl.QOM.Sum sum) {
                hasAgg = true;
                exprs.add(aggregateExpression(SelectExpr.AggFunc.SUM,
                        sum.$field(), outputName, noQuoteCtx, quotedCtx));
            } else if (f instanceof org.jooq.impl.QOM.Min<?> min) {
                hasAgg = true;
                exprs.add(aggregateExpression(SelectExpr.AggFunc.MIN,
                        min.$field(), outputName, noQuoteCtx, quotedCtx));
            } else if (f instanceof org.jooq.impl.QOM.Max<?> max) {
                hasAgg = true;
                exprs.add(aggregateExpression(SelectExpr.AggFunc.MAX,
                        max.$field(), outputName, noQuoteCtx, quotedCtx));
            } else {
                String inputColumn = noQuoteCtx.renderInlined(f).trim();
                exprs.add(new SelectExpr(SelectExpr.AggFunc.COLUMN, inputColumn, outputName));
            }
        }

        Condition where = select.$where();
        String filter = quotedCtx.renderInlined(where);

        return new ParquetQueryParser(schema, table, columns, filter, hasAgg, gbCols, exprs);
    }

    /**
     * Creates an aggregate expression while retaining safe SQL and referenced columns.
     *
     * @param func aggregate function
     * @param input aggregate input field
     * @param outputName aggregate output name
     * @param noQuoteCtx renderer for compatibility names
     * @param quotedCtx renderer for executable DuckDB SQL
     * @return parsed aggregate expression
     */
    private static SelectExpr aggregateExpression(SelectExpr.AggFunc func,
            org.jooq.Field<?> input, String outputName,
            DSLContext noQuoteCtx, DSLContext quotedCtx) {
        String inputColumn = noQuoteCtx.renderInlined(input).trim();
        String inputExpression = quotedCtx.renderInlined(input).trim();
        Integer decimalScale = func == SelectExpr.AggFunc.SUM
                && input.getDataType().isDecimal() ? input.getDataType().scale() : null;
        return new SelectExpr(func, inputColumn, inputExpression,
                referencedColumns(inputExpression), decimalScale, outputName);
    }

    /**
     * Extracts quoted physical column references from a rendered expression.
     *
     * @param expression quoted SQL expression
     * @return referenced column names in encounter order
     */
    private static List<String> referencedColumns(String expression) {
        List<String> result = new ArrayList<>();
        Matcher matcher = Pattern.compile("\"([^\"]+)\"").matcher(expression);
        while (matcher.find()) {
            String column = matcher.group(1).replace("\"\"", "\"");
            if (!result.contains(column)) {
                result.add(column);
            }
        }
        return result;
    }

    // ── JOIN helper methods ───────────────────────────────────────────────

    private static final String JOIN_ANY_RE = "(?:FROM|(?:(?:LEFT|RIGHT|FULL(?:\\s+OUTER)?|INNER|CROSS)\\s+)?JOIN)";

    /**
     * @param sql original SQL string
     * @param joinTables list of join tables with their aliases
     * @return SQL string with qualified table refs rewritten to aliases
     */
    static String rewriteTableRefs(String sql, List<JoinTable> joinTables) {
        String result = sql;
        for (JoinTable jt : joinTables) {
            String qualified = jt.schema() != null
                    ? jt.schema() + "." + jt.table() : jt.table();
            if (!jt.alias().equals(jt.table())) {
                result = result.replaceAll(
                        "(?i)(\\b" + JOIN_ANY_RE + "\\s+)" + Pattern.quote(qualified)
                                + "\\s+(?:AS\\s+)?" + Pattern.quote(jt.alias()),
                        "$1" + Matcher.quoteReplacement(jt.alias()));
            } else {
                result = result.replaceAll(
                        "(?i)(\\b" + JOIN_ANY_RE + "\\s+|,\\s*)" + Pattern.quote(qualified) + "\\b",
                        "$1" + Matcher.quoteReplacement(jt.table()));
            }
        }
        return result;
    }

    // ── public accessors ──────────────────────────────────────────────────

    /**
     * @return schema name, or null if not specified
     */
    public String getSchema() {
        return schema;
    }

    /**
     * @return table name
     */
    public String getTable() {
        return table;
    }

    /**
     * @return selected column names
     */
    public List<String> getColumns() {
        return columns;
    }

    /**
     * @return WHERE clause filter expression
     */
    public String getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        if (isJoin) {
            return "ParquetQueryParser{isJoin=true, joinTables=" + joinTables
                    + ", columns=" + columns + ", filter='" + filter + "'}";
        }
        return "ParquetQueryParser{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", columns=" + columns +
                ", filter='" + filter + '\'' +
                ", hasAggregation=" + hasAggregation +
                ", groupByColumnNames=" + groupByColumnNames +
                '}';
    }

}
