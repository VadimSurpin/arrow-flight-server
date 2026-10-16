package pro.surpin.data.arrowflight.trino;

import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Exposes Flight server schemas and tables to Trino and records scan pushdown. */
final class FlightMetadata implements ConnectorMetadata {
    private final FlightService service;
    private final Map<String, Map<String, Schema>> tableCache = new ConcurrentHashMap<>();

    /**
     * Creates metadata for one transaction.
     *
     * @param service Flight server access
     */
    FlightMetadata(FlightService service) {
        this.service = service;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return service.listSchemas().stream().map(FlightMetadata::lower).distinct().toList();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        Optional<String> remoteSchema = remoteSchema(tableName.getSchemaName());
        if (remoteSchema.isEmpty()) {
            return null;
        }
        return find(tables(remoteSchema.get()).keySet(), tableName.getTableName())
                .map(remoteTable -> FlightTableHandle.of(tableName, remoteSchema.get(), remoteTable))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        FlightTableHandle handle = (FlightTableHandle) table;
        return new ConnectorTableMetadata(handle.schemaTableName(), columns(handle).stream()
                .map(FlightColumnHandle::toMetadata).toList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        List<String> remoteSchemas = schemaName.map(name -> remoteSchema(name).stream().toList())
                .orElseGet(service::listSchemas);
        List<SchemaTableName> result = new ArrayList<>();
        for (String remoteSchema : remoteSchemas) {
            for (String remoteTable : tables(remoteSchema).keySet()) {
                result.add(new SchemaTableName(lower(remoteSchema), lower(remoteTable)));
            }
        }
        return result;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        columns((FlightTableHandle) table).forEach(column -> handles.put(column.name(), column));
        return handles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table,
                                           ColumnHandle column) {
        return ((FlightColumnHandle) column).toMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix) {
        Map<SchemaTableName, List<ColumnMetadata>> result = new LinkedHashMap<>();
        for (SchemaTableName name : listTables(session, prefix.getSchema())) {
            if (prefix.getTable().isPresent() && !prefix.getTable().get().equals(name.getTableName())) {
                continue;
            }
            FlightTableHandle handle = (FlightTableHandle) getTableHandle(session, name);
            if (handle != null) {
                result.put(name, columns(handle).stream().map(FlightColumnHandle::toMetadata).toList());
            }
        }
        return result;
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session, ConnectorTableHandle table, List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments) {
        FlightTableHandle handle = (FlightTableHandle) table;
        List<FlightColumnHandle> projected = assignments.values().stream()
                .map(FlightColumnHandle.class::cast).distinct().toList();
        if (projected.isEmpty()) {
            List<FlightColumnHandle> all = columns(handle);
            projected = all.isEmpty() ? List.of() : List.of(all.getFirst());
        }
        if (handle.columns().isPresent() && handle.columns().get().equals(projected)) {
            return Optional.empty();
        }
        List<Assignment> resultAssignments = assignments.entrySet().stream()
                .map(e -> new Assignment(e.getKey(), e.getValue(), ((FlightColumnHandle) e.getValue()).type()))
                .toList();
        return Optional.of(new ProjectionApplicationResult<>(handle.withColumns(projected), projections,
                resultAssignments, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        FlightTableHandle handle = (FlightTableHandle) table;
        TupleDomain<ColumnHandle> merged = handle.constraint().intersect(constraint.getSummary());
        if (merged.equals(handle.constraint())) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(handle.withConstraint(merged),
                constraint.getSummary(), false));
    }

    /**
     * Lists the readable columns of a table.
     *
     * @param handle table handle
     * @return column handles in table order
     * @throws IllegalStateException if the table no longer exists
     */
    List<FlightColumnHandle> columns(FlightTableHandle handle) {
        Schema schema = tables(handle.remoteSchema()).get(handle.remoteTable());
        if (schema == null) {
            throw new IllegalStateException("Flight table disappeared: " + handle.schemaTableName());
        }
        Map<String, FlightColumnHandle> columns = new LinkedHashMap<>();
        for (Field field : schema.getFields()) {
            Optional<Type> type = ArrowTypes.toTrino(field);
            String name = lower(field.getName());
            if (type.isPresent() && !columns.containsKey(name)) {
                columns.put(name, new FlightColumnHandle(name, field.getName(), type.get()));
            }
        }
        return List.copyOf(columns.values());
    }

    /**
     * Resolves a Trino schema name to the server name.
     *
     * @param schema Trino schema name
     * @return remote schema name, or empty when absent
     */
    private Optional<String> remoteSchema(String schema) {
        return find(service.listSchemas(), schema);
    }

    /**
     * Lists the tables of a schema once per transaction.
     *
     * @param remoteSchema remote schema name
     * @return remote table name to Arrow schema
     */
    private Map<String, Schema> tables(String remoteSchema) {
        return tableCache.computeIfAbsent(remoteSchema, service::listTables);
    }

    /**
     * Finds a remote name by exact match, then by case-insensitive match.
     *
     * @param remoteNames remote names
     * @param name Trino name
     * @return remote name, or empty when absent
     */
    private static Optional<String> find(Iterable<String> remoteNames, String name) {
        Optional<String> ignoringCase = Optional.empty();
        for (String remote : remoteNames) {
            if (remote.equals(name)) {
                return Optional.of(remote);
            }
            if (ignoringCase.isEmpty() && remote.equalsIgnoreCase(name)) {
                ignoringCase = Optional.of(remote);
            }
        }
        return ignoringCase;
    }

    /**
     * Lower-cases a name the way Trino does.
     *
     * @param name name
     * @return lower-case name
     */
    private static String lower(String name) {
        return name.toLowerCase(Locale.ENGLISH);
    }
}
