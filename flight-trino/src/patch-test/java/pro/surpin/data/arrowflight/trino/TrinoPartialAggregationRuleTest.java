package pro.surpin.data.arrowflight.trino;

import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.TypeManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.PushPartialAggregationIntoTableScan;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Checks state scans preserve native final aggregation for single and distributed plans. */
@Tag("unit")
class TrinoPartialAggregationRuleTest {
    /** Checks SINGLE becomes a native FINAL over connector states. */
    @Test
    void preservesNativeFinalForSingleAggregation() {
        Fixture fixture = new Fixture(AggregationNode.Step.SINGLE);
        var plan = fixture.apply().getTransformedPlan().orElseThrow();
        AggregationNode result = assertInstanceOf(AggregationNode.class, plan);
        assertEquals(AggregationNode.Step.FINAL, result.getStep());
        assertSame(fixture.function, result.getAggregations().values().iterator().next().getResolvedFunction());
        ProjectNode projection = assertInstanceOf(ProjectNode.class, result.getSource());
        TableScanNode scan = assertInstanceOf(TableScanNode.class, projection.getSource());
        assertTrue(scan.getTable().connectorHandle().producesPartialAggregation());
        assertEquals(Optional.of(false), scan.getUseConnectorNodePartitioning());
    }

    /** Checks replacing PARTIAL retains the symbol expected by the existing FINAL. */
    @Test
    void preservesIntermediateSymbolForPartialAggregation() {
        Fixture fixture = new Fixture(AggregationNode.Step.PARTIAL);
        ProjectNode result = assertInstanceOf(ProjectNode.class, fixture.apply().getTransformedPlan().orElseThrow());
        assertEquals(List.of(fixture.output), result.getOutputSymbols());
        assertInstanceOf(TableScanNode.class, result.getSource());
    }

    /** Checks a connector cannot return an unmarked raw scan as intermediate states. */
    @Test
    void rejectsUnmarkedStateScan() {
        Fixture fixture = new Fixture(AggregationNode.Step.SINGLE);
        when(fixture.handle.producesPartialAggregation()).thenReturn(false);
        assertThrows(IllegalStateException.class, fixture::apply);
    }

    /** Provides a built-in count and a connector-produced state scan. */
    private static final class Fixture {
        private final ResolvedFunction function = mock(ResolvedFunction.class);
        private final ConnectorTableHandle handle = mock(ConnectorTableHandle.class);
        private final Symbol output = new Symbol(BIGINT, "count");
        private final PlannerContext planner = mock(PlannerContext.class);
        private final Rule.Context context = mock(Rule.Context.class);
        private final Captures captures = mock(Captures.class);
        private final AggregationNode aggregation = mock(AggregationNode.class);

        /**
         * Creates a count pushdown fixture.
         *
         * @param step original aggregation step
         */
        private Fixture(AggregationNode.Step step) {
            Session session = mock(Session.class);
            Metadata metadata = mock(Metadata.class);
            TypeManager types = mock(TypeManager.class);
            BoundSignature signature = mock(BoundSignature.class);
            AggregationFunctionMetadata stateMetadata = mock(AggregationFunctionMetadata.class);
            TableScanNode scan = mock(TableScanNode.class);
            TableHandle original = mock(TableHandle.class);
            TableHandle pushed = mock(TableHandle.class);
            ColumnHandle stateColumn = mock(ColumnHandle.class);
            when(planner.getMetadata()).thenReturn(metadata);
            when(planner.getTypeManager()).thenReturn(types);
            when(context.getSession()).thenReturn(session);
            when(context.getSymbolAllocator()).thenReturn(new SymbolAllocator());
            when(context.getIdAllocator()).thenReturn(new PlanNodeIdAllocator());
            when(captures.get(any())).thenReturn(scan);
            when(scan.getTable()).thenReturn(original);
            when(scan.getAssignments()).thenReturn(Map.of());
            when(pushed.connectorHandle()).thenReturn(handle);
            when(handle.producesPartialAggregation()).thenReturn(true);
            when(signature.getName()).thenReturn(builtinFunctionName("count"));
            when(signature.getReturnType()).thenReturn(BIGINT);
            when(function.signature()).thenReturn(signature);
            when(stateMetadata.getIntermediateTypes()).thenReturn(List.of(BIGINT.getTypeSignature()));
            when(metadata.getAggregationFunctionMetadata(session, function)).thenReturn(stateMetadata);
            when(types.getType(BIGINT.getTypeSignature())).thenReturn(BIGINT);
            var count = new AggregationNode.Aggregation(function, List.of(), false, Optional.empty(), Optional.empty(), Optional.empty());
            when(aggregation.getAggregations()).thenReturn(Map.of(output, count));
            when(aggregation.getStep()).thenReturn(step);
            when(aggregation.getId()).thenReturn(new PlanNodeId("count"));
            when(aggregation.getHashSymbol()).thenReturn(Optional.empty());
            when(aggregation.getGroupIdSymbol()).thenReturn(Optional.empty());
            when(aggregation.getGroupingKeys()).thenReturn(List.of());
            when(aggregation.getGroupingSets()).thenReturn(new AggregationNode.GroupingSetDescriptor(List.of(), 1, Set.of(0)));
            when(aggregation.isDecomposable(session, metadata)).thenReturn(true);
            when(metadata.applyPartialAggregation(any(), any(), any(), any(), any(), any()))
                    .thenReturn(Optional.of(new AggregationApplicationResult<>(pushed,
                            List.of(new Variable("state", BIGINT)),
                            List.of(new Assignment("state", stateColumn, BIGINT)), Map.of(), false)));
        }

        /**
         * Applies the partial pushdown rule.
         *
         * @return rule result
         */
        private Rule.Result apply() {
            return new PushPartialAggregationIntoTableScan(planner).apply(aggregation, captures, context);
        }
    }
}
