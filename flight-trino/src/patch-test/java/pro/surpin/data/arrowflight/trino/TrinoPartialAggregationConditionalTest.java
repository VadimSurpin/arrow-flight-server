package pro.surpin.data.arrowflight.trino;

import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Checks conditional translation is opt-in and round-trips native expression structure. */
@Tag("unit")
class TrinoPartialAggregationConditionalTest {
    /** Checks disabling the bridge retains stock behavior for other connectors. */
    @Test
    void translatesConditionalsOnlyWhenEnabled() {
        var session = testSessionBuilder().build();
        var translator = new ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator(session, true);
        for (Expression expression : List.of(
                new Case(List.of(new WhenClause(new Constant(BOOLEAN, true), new Constant(BIGINT, 3L))),
                        new Constant(BIGINT, null)),
                new Coalesce(new Constant(BIGINT, null), new Constant(BIGINT, 7L)))) {
            assertTrue(ConnectorExpressionTranslator.translate(session, expression).isEmpty());
            var translated = translator.process(expression).orElseThrow();
            assertEquals(expression, ConnectorExpressionTranslator.translate(session, translated, mock(PlannerContext.class), Map.of()));
        }
    }
}
