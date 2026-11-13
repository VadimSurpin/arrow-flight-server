package pro.surpin.data.arrowflight.client.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests sharing filtered Spark V2 scans across scalar aggregate branches. */
class FlightScanSharingTest {
    private static SparkSession session;

    @TempDir
    Path directory;

    /** Installs the sharing rule over local Parquet relations for an optimizer integration test. */
    public static final class TestExtension extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> {
        @Override
        public BoxedUnit apply(SparkSessionExtensions extensions) {
            extensions.injectOptimizerRule(s -> new FlightScanSharing(r -> true));
            return BoxedUnit.UNIT;
        }
    }

    /** Starts a local Spark session with Parquet using the V2 relation path. */
    @BeforeAll
    static void startSession() {
        session = SparkSession.builder().master("local[1]").appName("scan-sharing-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.sources.useV1SourceList", "")
                .config("spark.sql.extensions", TestExtension.class.getName())
                .getOrCreate();
        SparkSession.setActiveSession(session);
    }

    /** Stops the local Spark session. */
    @AfterAll
    static void stopSession() {
        session.stop();
        SparkSession.clearActiveSession();
    }

    /** Three joined tables must retain their ON conditions and each aggregate's filter. */
    @Test
    void mergesJoinedBranchesAndPreservesResults() {
        session.range(1, 21).selectExpr("id AS sale_id", "id % 4 AS hour", "id % 3 AS demo_id",
                "id % 2 AS store_id").write().parquet(directory.resolve("sales").toString());
        session.range(0, 3).selectExpr("id AS demo_id", "id % 2 AS region")
                .write().parquet(directory.resolve("demo").toString());
        session.range(0, 2).selectExpr("id AS store_id", "id AS active")
                .write().parquet(directory.resolve("store").toString());
        session.read().format("parquet").load(directory.resolve("sales").toString())
                .createOrReplaceTempView("sharing_sales");
        session.read().format("parquet").load(directory.resolve("demo").toString())
                .createOrReplaceTempView("sharing_demo");
        session.read().format("parquet").load(directory.resolve("store").toString())
                .createOrReplaceTempView("sharing_store");
        try {
            String branch = "SELECT sum(sale_id) AS total FROM sharing_sales s"
                    + " JOIN sharing_demo d ON s.demo_id = d.demo_id"
                    + " JOIN sharing_store t ON s.store_id = t.store_id"
                    + " WHERE t.active = 1 AND d.region = 0 AND s.hour = %d";
            String sql = "SELECT a.total, b.total FROM (" + branch.formatted(1) + ") a"
                    + " CROSS JOIN (" + branch.formatted(2) + ") b";
            Dataset<Row> original = session.sql(sql);
            LogicalPlan plan = restoreRelations(original.queryExecution().optimizedPlan());
            session.conf().set(FlightScanSharing.CONF, "true");
            assertEquals("true", org.apache.spark.sql.internal.SQLConf.get()
                    .getConfString(FlightScanSharing.CONF, "false"));
            LogicalPlan rewritten = new FlightScanSharing(r -> true).apply(plan);
            assertTrue(rewritten.numberedTreeString().contains("Aggregate"));
            assertEquals(3, countRelations(rewritten), rewritten.numberedTreeString());
            assertEquals(original.collectAsList(), Dataset.ofRows(session, rewritten).collectAsList());
            Dataset<Row> automatic = session.sql(sql);
            assertEquals(3, countScans(automatic.queryExecution().optimizedPlan()),
                    automatic.queryExecution().optimizedPlan().numberedTreeString());
            assertEquals(original.collectAsList(), automatic.collectAsList());
        } finally {
            session.conf().unset(FlightScanSharing.CONF);
            session.catalog().dropTempView("sharing_sales");
            session.catalog().dropTempView("sharing_demo");
            session.catalog().dropTempView("sharing_store");
        }
    }

    private static int countRelations(LogicalPlan plan) {
        int count = plan instanceof DataSourceV2Relation ? 1 : 0;
        for (LogicalPlan child : scala.collection.JavaConverters.seqAsJavaListConverter(plan.children()).asJava()) {
            count += countRelations(child);
        }
        return count;
    }

    private static LogicalPlan restoreRelations(LogicalPlan plan) {
        if (plan instanceof DataSourceV2ScanRelation scan) {
            return scan.relation();
        }
        return plan.mapChildren(new AbstractFunction1<LogicalPlan, LogicalPlan>() {
            @Override
            public LogicalPlan apply(LogicalPlan child) {
                return restoreRelations(child);
            }
        });
    }

    private static int countScans(LogicalPlan plan) {
        int count = plan instanceof DataSourceV2ScanRelation ? 1 : 0;
        for (LogicalPlan child : scala.collection.JavaConverters.seqAsJavaListConverter(plan.children()).asJava()) {
            count += countScans(child);
        }
        return count;
    }
}
