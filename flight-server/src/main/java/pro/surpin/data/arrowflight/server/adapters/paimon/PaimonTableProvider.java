package pro.surpin.data.arrowflight.server.adapters.paimon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.server.TableProvider;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.stream.Stream;

/** Plans reads of Paimon tables stored in a filesystem warehouse. */
public final class PaimonTableProvider implements TableProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path warehouse;

    /**
     * Creates a provider for one Paimon filesystem warehouse.
     *
     * @param warehouse root directory holding Paimon databases
     */
    public PaimonTableProvider(Path warehouse) {
        if (warehouse == null) {
            throw new IllegalArgumentException("warehouse must not be null");
        }
        this.warehouse = warehouse;
    }

    /**
     * Returns the warehouse root of this provider.
     *
     * @return warehouse directory
     */
    public Path warehouse() {
        return warehouse;
    }

    @Override
    public TableScanPlan planScan(TableIdentifier table) throws IOException {
        if (table == null || isBlank(table.schema()) || isBlank(table.table())) {
            throw new IOException("Paimon table identifier must contain schema and table");
        }
        Path tableDir = tableDirectory(table);
        if (!Files.isDirectory(tableDir)) {
            throw new NoSuchFileException(tableDir.toString(), null, "Paimon table not found");
        }
        long snapshotId = latestSnapshot(tableDir);
        if (snapshotId < 0L) {
            long schemaId = PaimonSchemaParser.latestSchemaId(tableDir);
            return new TableScanPlan(PaimonSchemaParser.readSchema(tableDir, schemaId), new EmptyScan());
        }
        long schemaId = snapshotSchemaId(tableDir, snapshotId);
        Schema schema = PaimonSchemaParser.readSchema(tableDir, schemaId);
        URI tableUri = tableDir.toAbsolutePath().toUri();
        return new TableScanPlan(schema, new PaimonSnapshot(tableUri, snapshotId));
    }

    /**
     * Resolves the directory of a table inside the warehouse.
     * Directory names are matched case-insensitively because SQL identifiers are
     * case-insensitive by default.
     *
     * @param table logical table identifier
     * @return table directory
     * @throws IOException if the warehouse cannot be listed
     */
    public Path tableDirectory(TableIdentifier table) throws IOException {
        Path schemaDir = resolveIgnoreCase(warehouse, table.schema() + ".db");
        return resolveIgnoreCase(schemaDir, table.table());
    }

    /**
     * Resolves a child directory by exact name, then by case-insensitive match.
     *
     * @param parent parent directory
     * @param name child directory name
     * @return resolved child path, or the exact candidate when no directory matches
     * @throws IOException if the parent cannot be listed
     */
    private static Path resolveIgnoreCase(Path parent, String name) throws IOException {
        Path exact = parent.resolve(name);
        if (Files.isDirectory(exact) || !Files.isDirectory(parent)) {
            return exact;
        }
        try (Stream<Path> children = Files.list(parent)) {
            return children
                    .filter(Files::isDirectory)
                    .filter(child -> child.getFileName().toString().equalsIgnoreCase(name))
                    .findFirst()
                    .orElse(exact);
        }
    }

    /**
     * Checks whether a string is null or blank.
     *
     * @param value value to check
     * @return true when the value has no visible characters
     */
    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /**
     * Reads the latest snapshot identifier of a table.
     *
     * @param tableDir table directory
     * @return snapshot identifier, or -1 when the table has no snapshots
     * @throws IOException if the snapshot marker or snapshot file is unreadable
     */
    private static long latestSnapshot(Path tableDir) throws IOException {
        Path marker = tableDir.resolve("snapshot").resolve("LATEST");
        if (!Files.isRegularFile(marker)) {
            return -1L;
        }
        String content = Files.readString(marker).trim();
        if (content.isEmpty()) {
            return -1L;
        }
        long snapshotId;
        try {
            snapshotId = Long.parseLong(content);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed Paimon LATEST marker: " + content, e);
        }
        Path snapshotFile = tableDir.resolve("snapshot").resolve("snapshot-" + snapshotId);
        if (!Files.isRegularFile(snapshotFile)) {
            throw new IOException("Paimon snapshot file is missing: " + snapshotFile);
        }
        return snapshotId;
    }

    /**
     * Resolves the schema identifier referenced by a snapshot.
     *
     * @param tableDir table directory
     * @param snapshotId snapshot identifier
     * @return schema identifier of the snapshot, or the newest schema when absent
     * @throws IOException if the snapshot document is unreadable
     */
    private static long snapshotSchemaId(Path tableDir, long snapshotId) throws IOException {
        Path snapshotFile = tableDir.resolve("snapshot").resolve("snapshot-" + snapshotId);
        JsonNode root = MAPPER.readTree(Files.readString(snapshotFile));
        JsonNode schemaId = root.get("schemaId");
        if (schemaId == null || !schemaId.canConvertToLong()) {
            return PaimonSchemaParser.latestSchemaId(tableDir);
        }
        return schemaId.asLong();
    }
}
