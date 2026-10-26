package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.spark.read.FlightScan;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.PartialFunction;
import scala.collection.immutable.Nil$;
import scala.collection.immutable.Seq;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Spark session extension that switches adaptive query execution (AQE) off for the queries that would deadlock in it.
 *
 * <p>Runtime filtering keeps pushed filters in the plan ({@code spark.flight.retainPushedFilters}, on by default once
 * this extension is installed). When such a plan scans one table twice, Spark 3.5 can lock up: the thread that runs
 * the dynamic-pruning subquery holds the lock of a shared exchange while AQE's plan-update thread waits for it.
 * This guard sees every query at planning time and, for exactly those plans, sets {@code spark.sql.adaptive.enabled}
 * to false; a query listener restores it when the query ends. A warning is logged at start and at every switch.
 *
 * <p>Install it with {@code --conf spark.sql.extensions=pro.surpin.data.arrowflight.client.spark.FlightAqeGuard}.
 */
public final class FlightAqeGuard extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightAqeGuard.class);
    private static final String AQE = "spark.sql.adaptive.enabled";
    private static final String RETAIN = "spark.flight.retainPushedFilters";

    /**
     * Tells whether the guard is configured for the active session.
     *
     * @return true when {@code spark.sql.extensions} lists this class
     */
    public static boolean installed() {
        try {
            return SparkSession.active().conf().get("spark.sql.extensions", "").contains(FlightAqeGuard.class.getName());
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Tells whether a plan with these scans would deadlock in AQE: one of them keeps its filters and a table is
     * scanned more than once.
     *
     * @param scans the Flight scans of the plan, subqueries included
     * @return true when AQE has to be off for the query
     */
    public static boolean needsAqeOff(List<FlightScan> scans) {
        Map<String, Integer> counts = new HashMap<>();
        boolean retained = false;
        for (FlightScan scan : scans) {
            retained |= scan.retainsFilters();
            counts.merge(String.valueOf(scan.tableName()), 1, Integer::sum);
        }
        return retained && counts.values().stream().anyMatch(n -> n > 1);
    }

    @Override
    public BoxedUnit apply(SparkSessionExtensions extensions) {
        extensions.injectPlannerStrategy(session -> guard(session));
        return BoxedUnit.UNIT;
    }

    /** Planner strategy that plans nothing; it only decides whether AQE stays on for the query. */
    private static final class Guard extends SparkStrategy implements QueryExecutionListener {
        private final SparkSession session;
        // The plan node that switched AQE off; the query that contains it switches AQE back on.
        private LogicalPlan owner;

        Guard(SparkSession session) {
            this.session = session;
            LOGGER.warn("{} is managed automatically: with {}=auto (the default) this session may set {} to false for"
                    + " a query that scans one table twice with a retained runtime filter (Spark 3.5 AQE deadlock),"
                    + " and sets it back when the query ends", RETAIN, RETAIN, AQE);
            session.listenerManager().register(this);
        }

        @Override
        public Seq<SparkPlan> apply(LogicalPlan plan) {
            List<FlightScan> scans = scansOf(plan);
            if (!scans.isEmpty() && needsAqeOff(scans)) {
                synchronized (this) {
                    // The first plan Spark hands over is the whole query (ReturnAnswer); later calls are its parts and
                    // its subqueries' copies, which the finished query does not contain.
                    if (owner == null && aqeOn()) {
                        LOGGER.warn("Switching {} off for this query: it scans a table more than once and keeps a "
                                + "runtime filter, which can deadlock Spark's adaptive execution; it is switched "
                                + "back on when the query ends", AQE);
                        session.conf().set(AQE, "false");
                        owner = plan instanceof ReturnAnswer answer ? answer.child() : plan;
                    }
                }
            }
            return empty();
        }

        @Override
        public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
            restore(qe);
        }

        @Override
        public void onFailure(String funcName, QueryExecution qe, Exception exception) {
            restore(qe);
        }

        private void restore(QueryExecution qe) {
            synchronized (this) {
                LogicalPlan mine = owner;
                if (mine != null && qe.optimizedPlan().exists(p -> p == mine)) {
                    owner = null;
                    LOGGER.warn("Switching {} back on: the query that needed it off has ended", AQE);
                    session.conf().set(AQE, "true");
                }
            }
        }

        private boolean aqeOn() {
            return Boolean.parseBoolean(session.conf().get(AQE, "true"));
        }
    }

    // One guard per session: Spark builds planner strategies repeatedly, but the listener and the log are per session.
    private static final Map<SparkSession, Guard> GUARDS = new WeakHashMap<>();

    private static synchronized Guard guard(SparkSession session) {
        return GUARDS.computeIfAbsent(session, Guard::new);
    }

    @SuppressWarnings("unchecked")
    private static Seq<SparkPlan> empty() {
        return (Seq<SparkPlan>) (Seq<?>) Nil$.MODULE$;
    }

    private static List<FlightScan> scansOf(LogicalPlan plan) {
        PartialFunction<LogicalPlan, FlightScan> pick = new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(LogicalPlan node) {
                return node instanceof DataSourceV2ScanRelation r && r.scan() instanceof FlightScan;
            }

            @Override
            public <A1 extends LogicalPlan, B1> B1 applyOrElse(A1 node, scala.Function1<A1, B1> otherwise) {
                if (isDefinedAt(node)) {
                    return (B1) ((DataSourceV2ScanRelation) node).scan();
                }
                return otherwise.apply(node);
            }
        };
        return scala.collection.JavaConverters.seqAsJavaListConverter(plan.collectWithSubqueries(pick)).asJava();
    }
}
