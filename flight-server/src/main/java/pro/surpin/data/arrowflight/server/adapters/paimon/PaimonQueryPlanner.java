package pro.surpin.data.arrowflight.server.adapters.paimon;

import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.server.TableProvider.EmptyScan;
import pro.surpin.data.arrowflight.server.TableProvider.PaimonSnapshot;
import pro.surpin.data.arrowflight.server.TableProvider.ScanSource;
import pro.surpin.data.arrowflight.server.TableProvider.TableIdentifier;
import pro.surpin.data.arrowflight.server.TableProvider.TableScanPlan;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;

import java.io.IOException;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Plans Paimon SQL queries into DuckDB SQL over a pinned snapshot. */
public final class PaimonQueryPlanner {

    private static final Pattern FROM_TABLE = Pattern.compile(
            "\\bFROM\\s+(\"[^\"]+\"|`[^`]+`|[A-Za-z_][\\w$]*)\\s*\\.\\s*"
                    + "(\"[^\"]+\"|`[^`]+`|[A-Za-z_][\\w$]*)",
            Pattern.CASE_INSENSITIVE);

    private final PaimonTableProvider provider;
    private final MetadataService metadataService;

    /**
     * Result of planning a Paimon query: Arrow schema and DuckDB SQL.
     *
     * @param schema Arrow schema of the query result
     * @param duckSql DuckDB SQL reading the Paimon table
     */
    public record PlannedPaimonQuery(Schema schema, String duckSql) {
    }

    /**
     * Creates a planner over one Paimon warehouse.
     *
     * @param provider table provider for the warehouse
     * @param metadataService service that builds Arrow schemas
     */
    public PaimonQueryPlanner(PaimonTableProvider provider, MetadataService metadataService) {
        this.provider = provider;
        this.metadataService = metadataService;
    }

    /**
     * Creates a planner that rejects every query because Paimon is not the active format.
     *
     * @return disabled planner
     */
    public static PaimonQueryPlanner disabled() {
        return new PaimonQueryPlanner(null, null);
    }

    /**
     * Reports whether Paimon query planning is active.
     *
     * @return true when a warehouse provider is configured
     */
    public boolean enabled() {
        return provider != null;
    }

    /**
     * Plans a Paimon SQL query.
     *
     * @param sql SQL query text
     * @return result schema and DuckDB SQL reading the pinned snapshot
     * @throws IOException if the query is unsupported or the table cannot be read
     */
    public PlannedPaimonQuery plan(String sql) throws IOException {
        if (!enabled()) {
            throw new IOException("Paimon query planning is disabled");
        }
        ParquetQueryParser query = ParquetQueryParser.parse(sql);
        if (query.isJoin) {
            throw new IOException("Joins are not supported for Paimon queries");
        }
        if (query.schema == null || query.schema.isBlank()) {
            throw new IOException("Paimon queries must reference schema.table");
        }
        TableIdentifier table = new TableIdentifier(query.schema, query.table);
        TableScanPlan scan = provider.planScan(table);
        String duckSql = rewriteFrom(sql, table, scan.source());
        Schema schema = metadataService.getQuerySchema(query, scan.tableSchema());
        return new PlannedPaimonQuery(schema, duckSql);
    }

    /**
     * Replaces the Paimon table reference in the FROM clause with a native scan expression.
     *
     * @param sql SQL query text
     * @param table planned table identifier
     * @param source planned scan source
     * @return DuckDB SQL over the Paimon snapshot
     * @throws IOException if the table reference is missing or the source is unsupported
     */
    private String rewriteFrom(String sql, TableIdentifier table, ScanSource source)
            throws IOException {
        Matcher matcher = FROM_TABLE.matcher(sql);
        if (!matcher.find()) {
            throw new IOException("Unable to locate the Paimon table in the query");
        }
        String from;
        if (source instanceof PaimonSnapshot snapshot) {
            from = PaimonScanSql.fromClause(snapshot);
        } else if (source instanceof EmptyScan) {
            Path tableDir = provider.tableDirectory(table);
            from = PaimonScanSql.fromClause(tableDir);
        } else {
            throw new IOException("Unsupported Paimon scan source: "
                    + source.getClass().getSimpleName());
        }
        return sql.substring(0, matcher.start()) + "FROM " + from + sql.substring(matcher.end());
    }
}
