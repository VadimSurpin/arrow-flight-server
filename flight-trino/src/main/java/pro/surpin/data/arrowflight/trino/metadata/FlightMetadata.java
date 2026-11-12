package pro.surpin.data.arrowflight.trino.metadata;

import pro.surpin.data.arrowflight.trino.client.FlightService;
import pro.surpin.data.arrowflight.trino.type.ArrowTypes;
import pro.surpin.data.arrowflight.trino.pushdown.FlightFilters;

import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
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
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Exposes Flight server schemas and tables to Trino and records scan pushdown. */
public final class FlightMetadata implements ConnectorMetadata {
    private final FlightService service;
    private final String columnQuote;
    private final Map<String, Map<String, Schema>> tableCache = new ConcurrentHashMap<>();

    /**
     * Creates metadata for one transaction.
     *
     * @param service Flight server access
     */
    public FlightMetadata(FlightService service) {
        this(service, "");
    }

    /**
     * Creates metadata for one transaction.
     *
     * @param service Flight server access
     * @param columnQuote identifier quote used in generated SQL
     */
    public FlightMetadata(FlightService service, String columnQuote) {
        this.service = service;
        this.columnQuote = columnQuote;
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
    public TableStatistics getTableStatistics(ConnectorSession session,
            ConnectorTableHandle table) {
        FlightTableHandle handle = (FlightTableHandle) table;
        if (handle.aggregation().isPresent()) {
            return TableStatistics.empty();
        }
        long rows = service.estimate(
                pro.surpin.data.arrowflight.trino.pushdown.FlightQueries.select(handle, columnQuote))
                .totalRecords();
        if (rows < 0) {
            return TableStatistics.empty();
        }
        return TableStatistics.builder().setRowCount(Estimate.of(rows)).build();
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
        if (handle.aggregation().isPresent()) {
            return Optional.empty();
        }
        Set<String> variables = new LinkedHashSet<>();
        projections.forEach(expression -> collectVariables(expression, variables));
        List<FlightColumnHandle> projected = variables.stream()
                .map(assignments::get).map(FlightColumnHandle.class::cast).distinct().toList();
        if (projected.isEmpty()) {
            List<FlightColumnHandle> all = columns(handle);
            projected = all.isEmpty() ? List.of() : List.of(all.getFirst());
        }
        if (handle.columns().isPresent() && handle.columns().get().equals(projected)) {
            return Optional.empty();
        }
        List<Assignment> resultAssignments = variables.stream()
                .map(name -> new Assignment(name, assignments.get(name),
                        ((FlightColumnHandle) assignments.get(name)).type()))
                .toList();
        return Optional.of(new ProjectionApplicationResult<>(handle.withColumns(projected), projections,
                resultAssignments, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        FlightTableHandle handle = (FlightTableHandle) table;
        if (handle.aggregation().isPresent()) {
            return Optional.empty();
        }
        FlightFilters.Pushdown filters = FlightFilters.split(constraint.getSummary());
        TupleDomain<ColumnHandle> merged = handle.constraint().intersect(filters.pushed());
        if (merged.equals(handle.constraint())) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(handle.withConstraint(merged),
                filters.remaining(), constraint.getExpression(), false));
    }

    /**
     * Applies supported aggregates with Trino-compatible intermediate types.
     *
     * @param session connector session
     * @param table input table
     * @param aggregates requested aggregates
     * @param assignments input variables and columns
     * @param groupingSets requested grouping sets
     * @param intermediateTypes expected state types in aggregate order
     * @return partial scan, or empty when unsupported
     */
    // The additional SPI method is supplied by trino-fork/flight-connector.patch.
    // Omitting @Override keeps the connector buildable against stock Trino 446.
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyPartialAggregation(
            ConnectorSession session, ConnectorTableHandle table, List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets,
            List<Type> intermediateTypes) {
        FlightTableHandle handle = (FlightTableHandle) table;
        if (!Boolean.TRUE.equals(session.getProperty("partial_aggregation_pushdown", Boolean.class))
                || handle.aggregation().isPresent() || groupingSets.size() != 1 || aggregates.isEmpty()
                || intermediateTypes.size() != aggregates.size()) {
            return Optional.empty();
        }
        List<FlightColumnHandle> groups = new ArrayList<>();
        for (ColumnHandle group : groupingSets.getFirst()) {
            if (!(group instanceof FlightColumnHandle column) || !supportsGrouping(column.type())) {
                return Optional.empty();
            }
            groups.add(column);
        }
        List<AggregateSpec> specifications = new ArrayList<>();
        List<ConnectorExpression> projections = new ArrayList<>();
        List<Assignment> outputs = new ArrayList<>();
        Set<String> names = new LinkedHashSet<>();
        groups.forEach(column -> names.add(column.remoteName().toLowerCase(Locale.ROOT)));
        for (int i = 0; i < groups.size(); i++) {
            FlightColumnHandle group = groups.get(i);
            outputs.add(new Assignment("group_" + i, group, group.type()));
        }
        for (int i = 0; i < aggregates.size(); i++) {
            AggregateFunction aggregate = aggregates.get(i);
            if (aggregate.isDistinct()
                    || !aggregate.getSortItems().isEmpty() || aggregate.getFilter().isPresent()
                    || aggregate.getArguments().size() > 1) {
                return Optional.empty();
            }
            Optional<FlightColumnHandle> input = Optional.empty();
            if (!aggregate.getArguments().isEmpty()) {
                if (!(aggregate.getArguments().getFirst() instanceof Variable variable)
                        || !(assignments.get(variable.getName()) instanceof FlightColumnHandle column)
                        || !variable.getType().equals(column.type())) {
                    return Optional.empty();
                }
                input = Optional.of(column);
            }
            AggregateSpec.Function function;
            try {
                function = AggregateSpec.Function.valueOf(aggregate.getFunctionName().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ignored) {
                return Optional.empty();
            }
            Optional<Type> stateType = AggregateSpec.intermediateType(function, input);
            if (stateType.isEmpty() || !stateType.orElseThrow().equals(intermediateTypes.get(i))) {
                return Optional.empty();
            }
            String name = "__flight_" + aggregate.getFunctionName().toLowerCase(Locale.ROOT) + "_" + i;
            AggregateSpec specification;
            List<String> reserved;
            while (true) {
                FlightColumnHandle output = new FlightColumnHandle(name, name, stateType.orElseThrow());
                specification = new AggregateSpec(function, input, output);
                if (!specification.resultType().equals(aggregate.getOutputType())) {
                    return Optional.empty();
                }
                reserved = new ArrayList<>(specification.remoteAggregates().stream()
                        .map(remote -> remote.output().remoteName().toLowerCase(Locale.ROOT)).toList());
                reserved.add(name.toLowerCase(Locale.ROOT));
                if (reserved.stream().noneMatch(names::contains)) {
                    break;
                }
                name += "_";
            }
            names.addAll(reserved);
            specifications.add(specification);
            outputs.add(new Assignment(name, specification.output(), specification.output().type()));
            projections.add(new Variable(name, specification.output().type()));
        }
        FlightAggregation aggregation = new FlightAggregation(groups, specifications);
        return Optional.of(new AggregationApplicationResult<>(handle.withAggregation(aggregation),
                projections, outputs, Map.of(), false));
    }

    /**
     * Checks types supported by the initial count contract.
     *
     * @param type input type
     * @return whether the type has supported equality and null semantics
     */
    private static boolean supportsGrouping(Type type) {
        return type instanceof BigintType || type instanceof IntegerType || type instanceof SmallintType
                || type instanceof TinyintType || type instanceof BooleanType || type instanceof VarcharType;
    }

    /**
     * Collects input variables used by a projection expression.
     *
     * @param expression projection expression
     * @param variables referenced variable names
     */
    private static void collectVariables(ConnectorExpression expression, Set<String> variables) {
        if (expression instanceof Variable variable) {
            variables.add(variable.getName());
        } else {
            expression.getChildren().forEach(child -> collectVariables(child, variables));
        }
    }

    /**
     * Lists the readable columns of a table.
     *
     * @param handle table handle
     * @return column handles in table order
     * @throws IllegalStateException if the table no longer exists
     */
    List<FlightColumnHandle> columns(FlightTableHandle handle) {
        if (handle.aggregation().isPresent()) {
            return handle.aggregation().orElseThrow().outputColumns();
        }
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
