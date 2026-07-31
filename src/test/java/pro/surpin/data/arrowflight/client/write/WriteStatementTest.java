package pro.surpin.data.arrowflight.client.write;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WriteStatementTest {

    // ── numberToDecimal256 ──────────────────────────────────────────────────

    @Test
    void numberToDecimal256NonNull() {
        String result = WriteStatement.castNumber(42, "decimal(10,2)");
        assertEquals("cast(42 as decimal(10,2))", result);
    }

    @Test
    void numberToDecimal256Null() {
        String result = WriteStatement.castNumber(null, "decimal");
        assertEquals("cast(null as decimal)", result);
    }

    // ── primitiveToVarchar ──────────────────────────────────────────────────

    @Test
    void primitiveToVarcharNonNull() {
        String result = WriteStatement.primitiveToVarchar("hello", "varchar");
        assertEquals("'hello'", result);
    }

    @Test
    void primitiveToVarcharNull() {
        String result = WriteStatement.primitiveToVarchar(null, "varchar");
        assertEquals("cast(null as varchar)", result);
    }

    @Test
    void primitiveToVarcharEscapesQuote() {
        String result = WriteStatement.primitiveToVarchar("it's", "varchar");
        assertEquals("'it''s'", result);
    }

    // ── booleanToBit ────────────────────────────────────────────────────────

    @Test
    void booleanToBitTrue() {
        String result = WriteStatement.booleanToBit(true, "bit");
        assertEquals("cast(true as bit)", result);
    }

    @Test
    void booleanToBitNull() {
        String result = WriteStatement.booleanToBit(null, "bit");
        assertEquals("cast(null as bit)", result);
    }

    // ── numberToBit ─────────────────────────────────────────────────────────

    @Test
    void numberToBitNonZero() {
        String result = WriteStatement.numberToBit(1, "bit");
        assertEquals("cast(true as bit)", result);
    }

    @Test
    void numberToBitZero() {
        String result = WriteStatement.numberToBit(0, "bit");
        assertEquals("cast(false as bit)", result);
    }

    @Test
    void numberToBitNull() {
        String result = WriteStatement.numberToBit(null, "bit");
        assertEquals("cast(null as bit)", result);
    }

    // ── stringToBit ─────────────────────────────────────────────────────────

    @Test
    void stringToBitTrue() {
        String result = WriteStatement.stringToBit("true", "bit");
        assertEquals("cast(true as bit)", result);
    }

    @Test
    void stringToBitFalse() {
        String result = WriteStatement.stringToBit("FALSE", "bit");
        assertEquals("cast(false as bit)", result);
    }

    @Test
    void stringToBitArbitraryText() {
        String result = WriteStatement.stringToBit("xyz", "bit");
        assertEquals("cast(false as bit)", result);
    }
}
