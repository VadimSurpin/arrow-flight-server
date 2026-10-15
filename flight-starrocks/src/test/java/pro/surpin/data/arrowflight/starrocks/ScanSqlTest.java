package pro.surpin.data.arrowflight.starrocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ScanSqlTest {
    @Test
    void buildsFullAndProjectedQueries() {
        assertEquals("SELECT * FROM sales.orders",
                ScanSql.fullTable("sales", "orders"));
        assertEquals("SELECT id, amount FROM sales.orders",
                ScanSql.select("sales", "orders", List.of("id", "amount")));
    }

    @Test
    void rejectsInjectedIdentifiers() {
        assertThrows(IllegalArgumentException.class,
                () -> ScanSql.fullTable("sales; DROP TABLE users", "orders"));
        assertThrows(IllegalArgumentException.class,
                () -> ScanSql.select("sales", "orders", List.of("id; DELETE")));
    }
}
