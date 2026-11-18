package pro.surpin.data.arrowflight.server.adapters.paimon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Reads Paimon schema JSON files and converts them to Arrow schemas. */
public final class PaimonSchemaParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern SCHEMA_FILE = Pattern.compile("schema-(\\d+)");
    private static final Pattern TIMESTAMP = Pattern.compile(
            "TIMESTAMP(?:_WITH_LOCAL_TIME_ZONE)?\\((\\d)\\)");
    private static final Pattern DECIMAL = Pattern.compile(
            "DECIMAL\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");
    private static final Pattern CHARACTER = Pattern.compile(
            "(?:VAR)?CHAR\\s*(?:\\(\\s*(\\d+)\\s*\\))?");

    /** Creates no instances of the parser. */
    private PaimonSchemaParser() {
    }

    /**
     * Reads the schema file with the given identifier from a Paimon table directory.
     *
     * @param tableDir Paimon table directory
     * @param schemaId identifier of the schema to read
     * @return Arrow schema of the selected schema version
     * @throws IOException if the schema file is missing or malformed
     */
    public static Schema readSchema(Path tableDir, long schemaId) throws IOException {
        Path file = tableDir.resolve("schema").resolve("schema-" + schemaId);
        if (!Files.isRegularFile(file)) {
            throw new IOException("Paimon schema file is missing: " + file);
        }
        return parse(Files.readString(file));
    }

    /**
     * Finds the newest schema identifier written for a table.
     *
     * @param tableDir Paimon table directory
     * @return greatest schema identifier
     * @throws IOException if no schema file is present
     */
    public static long latestSchemaId(Path tableDir) throws IOException {
        Path schemaDir = tableDir.resolve("schema");
        if (!Files.isDirectory(schemaDir)) {
            throw new IOException("Paimon schema directory is missing: " + schemaDir);
        }
        long latest = -1L;
        try (Stream<Path> files = Files.list(schemaDir)) {
            for (Path file : files.toList()) {
                Matcher matcher = SCHEMA_FILE.matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    latest = Math.max(latest, Long.parseLong(matcher.group(1)));
                }
            }
        }
        if (latest < 0L) {
            throw new IOException("No Paimon schema file found in " + schemaDir);
        }
        return latest;
    }

    /**
     * Converts Paimon schema JSON into an Arrow schema.
     *
     * @param json Paimon schema document
     * @return Arrow schema
     * @throws IOException if the document is malformed or uses an unsupported type
     */
    public static Schema parse(String json) throws IOException {
        JsonNode root = MAPPER.readTree(json);
        JsonNode fields = root.path("fields");
        if (!fields.isArray()) {
            throw new IOException("Paimon schema has no fields array");
        }
        List<Field> arrowFields = new ArrayList<>(fields.size());
        for (JsonNode field : fields) {
            String name = field.path("name").asText(null);
            String type = field.path("type").asText(null);
            if (name == null || type == null) {
                throw new IOException("Paimon schema field has no name or type");
            }
            boolean nullable = !field.has("nullable") || field.get("nullable").asBoolean();
            arrowFields.add(new Field(name, new FieldType(nullable, arrowType(type, name), null), null));
        }
        return new Schema(arrowFields);
    }

    /**
     * Maps a Paimon column type name to an Arrow type.
     *
     * @param type Paimon type name, for example BIGINT or DECIMAL(10, 2)
     * @param column column name used in error messages
     * @return matching Arrow type
     * @throws IOException if the type is not supported
     */
    static ArrowType arrowType(String type, String column) throws IOException {
        String normalized = type.trim().toUpperCase(java.util.Locale.ROOT);
        switch (normalized) {
            case "BOOLEAN":
                return ArrowType.Bool.INSTANCE;
            case "TINYINT":
                return new ArrowType.Int(8, true);
            case "SMALLINT":
                return new ArrowType.Int(16, true);
            case "INT":
            case "INTEGER":
                return new ArrowType.Int(32, true);
            case "BIGINT":
                return new ArrowType.Int(64, true);
            case "FLOAT":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "DOUBLE":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "DATE":
                return new ArrowType.Date(DateUnit.DAY);
            case "BINARY":
            case "VARBINARY":
            case "BYTES":
                return ArrowType.Binary.INSTANCE;
            case "STRING":
                return ArrowType.Utf8.INSTANCE;
            default:
                return complexType(normalized, column);
        }
    }

    private static ArrowType complexType(String normalized, String column) throws IOException {
        Matcher timestamp = TIMESTAMP.matcher(normalized);
        if (timestamp.matches()) {
            TimeUnit unit = Integer.parseInt(timestamp.group(1)) <= 3
                    ? TimeUnit.MILLISECOND : TimeUnit.MICROSECOND;
            String zone = normalized.contains("LOCAL_TIME_ZONE") ? "UTC" : null;
            return new ArrowType.Timestamp(unit, zone);
        }
        Matcher decimal = DECIMAL.matcher(normalized);
        if (decimal.matches()) {
            return new ArrowType.Decimal(Integer.parseInt(decimal.group(1)),
                    Integer.parseInt(decimal.group(2)), 128);
        }
        if (CHARACTER.matcher(normalized).matches()) {
            return ArrowType.Utf8.INSTANCE;
        }
        throw new IOException("Unsupported Paimon type " + normalized
                + " for column " + column);
    }
}
