package pro.surpin.data.arrowflight.trino.pushdown;

import pro.surpin.data.arrowflight.trino.metadata.FlightColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks exact scalar pushdown refuses overflow and incompatible floating comparisons. */
@Tag("unit")
class TypedExpressionsTest {
    /** Checks conditionals require total branches and retain native output types. */
    @Test
    void rendersSafeConditionals() {
        var value = new Variable("v", BIGINT);
        Map<String, ColumnHandle> columns = Map.of("v", new FlightColumnHandle("v", "v", BIGINT));
        var coalesce = new Call(BIGINT, new io.trino.spi.expression.FunctionName("$trino_coalesce"),
                List.of(value, new Constant(0L, BIGINT)));
        assertTrue(TypedExpressions.translate(coalesce, columns, "\"").orElseThrow().contains("COALESCE("));
        var condition = new Call(BOOLEAN, StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(value, new Constant(0L, BIGINT)));
        var select = new Call(BIGINT, new io.trino.spi.expression.FunctionName("$trino_case"),
                List.of(condition, value, new Constant(null, BIGINT)));
        assertTrue(TypedExpressions.translate(select, columns, "\"").orElseThrow().contains("CASE WHEN"));
        var unsafe = new Call(BIGINT, StandardFunctions.CAST_FUNCTION_NAME,
                List.of(new Constant(io.airlift.slice.Slices.utf8Slice("bad"), VARCHAR)));
        assertTrue(TypedExpressions.translate(new Call(BIGINT, new io.trino.spi.expression.FunctionName("$trino_case"),
                List.of(new Constant(false, BOOLEAN), unsafe, value)), columns, "\"").isEmpty());
    }
    /** Checks decimal intermediate scales survive product rendering. */
    @Test
    void rendersExactProduct() {
        var price = DecimalType.createDecimalType(18, 2);
        var discount = DecimalType.createDecimalType(9, 2);
        var difference = DecimalType.createDecimalType(10, 2);
        var output = DecimalType.createDecimalType(28, 4);
        var subtract = new Call(difference, StandardFunctions.SUBTRACT_FUNCTION_NAME,
                List.of(new Constant(100L, discount), new Variable("discount", discount)));
        var product = new Call(output, StandardFunctions.MULTIPLY_FUNCTION_NAME,
                List.of(new Variable("price", price), subtract));
        Map<String, ColumnHandle> columns = Map.of("price", new FlightColumnHandle("price", "Price", price),
                "discount", new FlightColumnHandle("discount", "Discount", discount));
        String sql = TypedExpressions.translate(product, columns, "\"").orElseThrow();
        assertTrue(sql.contains("\"Price\""));
        assertTrue(sql.contains("AS DECIMAL(10,2)"));
        assertTrue(sql.contains("AS DECIMAL(38,2)"));
        assertTrue(sql.endsWith("AS DECIMAL(28,4))"));
    }

    /** Checks native overflow, rounding and NaN semantics stay in Trino. */
    @Test
    void refusesUnsafeEvaluation() {
        var integer = new Variable("v", BIGINT);
        Map<String, ColumnHandle> columns = Map.of("v", new FlightColumnHandle("v", "v", BIGINT));
        assertTrue(TypedExpressions.translate(new Call(BIGINT, StandardFunctions.ADD_FUNCTION_NAME,
                List.of(integer, integer)), columns, "\"").isEmpty());
        assertTrue(TypedExpressions.translate(new Call(INTEGER, StandardFunctions.CAST_FUNCTION_NAME,
                List.of(integer)), columns, "\"").isEmpty());
        assertTrue(TypedExpressions.translate(new Call(BIGINT, StandardFunctions.CAST_FUNCTION_NAME,
                List.of(new Constant(io.airlift.slice.Slices.utf8Slice("bad"), VARCHAR))), columns, "\"").isEmpty());
        var floating = new Variable("d", DOUBLE);
        assertTrue(TypedExpressions.translate(new Call(BOOLEAN, StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(floating, new Constant(0.0, DOUBLE))),
                Map.of("d", new FlightColumnHandle("d", "d", DOUBLE)), "\"").isEmpty());
    }
}
