package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import pro.surpin.data.arrowflight.client.spark.read.FlightScan;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the planner extension that switches adaptive execution off for plans that would deadlock in it.
 */
class FlightAqeGuardTest {
    private static SparkSession session;

    @BeforeAll
    static void startSession() {
        session = SparkSession.builder().master("local[1]").appName("aqe-guard")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.extensions", FlightAqeGuard.class.getName())
                .getOrCreate();
    }

    @AfterAll
    static void stopSession() {
        session.stop();
    }

    private static FlightScan scan(String name, boolean retained) {
        Table table = mock(Table.class);
        when(table.getName()).thenReturn(name);
        return new FlightScan(mock(Configuration.class), table, false, null,
                new org.apache.spark.sql.sources.Filter[0],
                new org.apache.spark.sql.connector.expressions.filter.Predicate[0], retained);
    }

    private static LogicalPlan relation(FlightScan scan, String column) {
        AttributeReference attribute = new AttributeReference(column, DataTypes.LongType, true, Metadata.empty(),
                org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId(),
                JavaConverters.asScalaBuffer(List.<String>of()).toList());
        var output = JavaConverters.asScalaBuffer(List.of(attribute)).toList();
        org.apache.spark.sql.connector.catalog.Table v2 = mock(org.apache.spark.sql.connector.catalog.Table.class);
        String name = scan.tableName();
        when(v2.name()).thenReturn(name);
        DataSourceV2Relation relation = new DataSourceV2Relation(v2, output, Option.empty(), Option.empty(),
                new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Map.of()));
        return new DataSourceV2ScanRelation(relation, (Scan) scan, output, Option.empty(), Option.empty());
    }

    /** A table scanned twice with one scan keeping its filters needs AQE off. */
    @Test
    void asksForAqeOffOnlyForRetainedDuplicates() {
        assertTrue(FlightAqeGuard.needsAqeOff(List.of(scan("t", true), scan("t", false))));
        assertFalse(FlightAqeGuard.needsAqeOff(List.of(scan("t", false), scan("t", false))), "nothing retained");
        assertFalse(FlightAqeGuard.needsAqeOff(List.of(scan("a", true), scan("b", false))), "no table twice");
        assertFalse(FlightAqeGuard.needsAqeOff(List.of()));
    }

    /** Planning such a query switches AQE off and the finished query switches it back on. */
    @Test
    void switchesAqeOffWhilePlanningAndBackOnAfterwards() {
        LogicalPlan join = new Join(relation(scan("t", true), "a"), relation(scan("t", false), "b"),
                org.apache.spark.sql.catalyst.plans.Inner$.MODULE$, Option.empty(),
                org.apache.spark.sql.catalyst.plans.logical.JoinHint.NONE());
        assertEquals("true", session.conf().get("spark.sql.adaptive.enabled"));

        var strategies = JavaConverters.seqAsJavaListConverter(session.sessionState().planner().strategies()).asJava();
        var guard = strategies.stream().filter(x -> x.getClass().getName().contains("FlightAqeGuard")).findFirst()
                .orElseThrow();
        guard.apply(new org.apache.spark.sql.catalyst.plans.logical.ReturnAnswer(join));
        guard.apply(join);
        assertEquals("false", session.conf().get("spark.sql.adaptive.enabled"));

        QueryExecution other = mock(QueryExecution.class);
        LogicalPlan unrelated = relation(scan("u", false), "c");
        when(other.optimizedPlan()).thenReturn(unrelated);
        ((org.apache.spark.sql.util.QueryExecutionListener) guard).onSuccess("collect", other, 1L);
        assertEquals("false", session.conf().get("spark.sql.adaptive.enabled"), "another query's end changes nothing");

        QueryExecution mine = mock(QueryExecution.class);
        when(mine.optimizedPlan()).thenReturn(join);
        ((org.apache.spark.sql.util.QueryExecutionListener) guard).onSuccess("collect", mine, 1L);
        assertEquals("true", session.conf().get("spark.sql.adaptive.enabled"));
    }
}
