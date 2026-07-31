package pro.surpin.data.arrowflight.client.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QueryStatementTest {

    @Test
    void getStatementSelectOnly() {
        QueryStatement qs = new QueryStatement("SELECT * FROM table", null, null);
        assertEquals("SELECT * FROM table  ", qs.getStatement());
    }

    @Test
    void getStatementWithWhere() {
        QueryStatement qs = new QueryStatement("SELECT * FROM table", "id > 5", null);
        assertEquals("SELECT * FROM table where id > 5 ", qs.getStatement());
    }

    @Test
    void getStatementWithGroupBy() {
        QueryStatement qs = new QueryStatement("SELECT count(*)", null, "col");
        assertEquals("SELECT count(*)  group by col", qs.getStatement());
    }

    @Test
    void getStatementWithWhereAndGroupBy() {
        QueryStatement qs = new QueryStatement("SELECT count(*)", "id > 0", "col");
        assertEquals("SELECT count(*) where id > 0 group by col", qs.getStatement());
    }

    @Test
    void getStatementEmptyWhereOmitted() {
        QueryStatement qs = new QueryStatement("SELECT 1", "", "");
        assertEquals("SELECT 1  ", qs.getStatement());
    }

    @Test
    void differentNull() {
        QueryStatement qs = new QueryStatement("SELECT * FROM t", null, null);
        assertTrue(qs.different(null));
    }

    @Test
    void differentSameIsFalse() {
        QueryStatement a = new QueryStatement("SELECT * FROM t", "id > 0", "col");
        QueryStatement b = new QueryStatement("SELECT * FROM t", "id > 0", "col");
        assertFalse(a.different(b));
    }

    @Test
    void differentSelect() {
        QueryStatement a = new QueryStatement("SELECT a", null, null);
        QueryStatement b = new QueryStatement("SELECT b", null, null);
        assertTrue(a.different(b));
    }

    @Test
    void differentSelectCaseInsensitive() {
        QueryStatement a = new QueryStatement("SELECT * FROM t", null, null);
        QueryStatement b = new QueryStatement("select * from t", null, null);
        assertFalse(a.different(b));
    }

    @Test
    void differentWhere() {
        QueryStatement a = new QueryStatement("SELECT 1", "x = 1", null);
        QueryStatement b = new QueryStatement("SELECT 1", "x = 2", null);
        assertTrue(a.different(b));
    }

    @Test
    void differentWhereNullVsNonNull() {
        QueryStatement a = new QueryStatement("SELECT 1", null, null);
        QueryStatement b = new QueryStatement("SELECT 1", "x = 1", null);
        assertTrue(a.different(b));
    }

    @Test
    void differentGroupBy() {
        QueryStatement a = new QueryStatement("SELECT 1", null, "a");
        QueryStatement b = new QueryStatement("SELECT 1", null, "b");
        assertTrue(a.different(b));
    }

    @Test
    void differentGroupByNullVsNonNull() {
        QueryStatement a = new QueryStatement("SELECT 1", null, null);
        QueryStatement b = new QueryStatement("SELECT 1", null, "a");
        assertTrue(a.different(b));
    }

    @Test
    void differentShortCircuitsOnSelect() {
        QueryStatement a = new QueryStatement("SELECT a", "w", "g");
        QueryStatement b = new QueryStatement("SELECT b", "w", "g");
        assertTrue(a.different(b));
    }
}
