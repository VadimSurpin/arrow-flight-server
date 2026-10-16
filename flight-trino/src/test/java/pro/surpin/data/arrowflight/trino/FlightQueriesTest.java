package pro.surpin.data.arrowflight.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class FlightQueriesTest {
    private static final FlightColumnHandle ID = new FlightColumnHandle("id", "ID", BigintType.BIGINT);
    private static final FlightColumnHandle NAME = new FlightColumnHandle("name", "Name", VarcharType.VARCHAR);
    private static final FlightTableHandle TABLE =
            FlightTableHandle.of(new SchemaTableName("sales", "orders"), "Sales", "Orders");

    @Test
    void selectsWholeTableWithoutPushdown() {
        assertEquals("select * from Sales.Orders", FlightQueries.select(TABLE, ""));
    }

    @Test
    void selectsProjectedColumnsWithFilter() {
        TupleDomain<ColumnHandle> filter = TupleDomain.withColumnDomains(
                Map.of(ID, Domain.singleValue(BigintType.BIGINT, 7L)));
        FlightTableHandle handle = TABLE.withColumns(List.of(ID, NAME)).withConstraint(filter);
        assertEquals("select ID,Name from Sales.Orders where ID = 7", FlightQueries.select(handle, ""));
    }

    @Test
    void quotesIdentifiersWhenConfigured() {
        FlightTableHandle handle = TABLE.withColumns(List.of(NAME));
        assertEquals("select \"Name\" from \"Sales\".\"Orders\"", FlightQueries.select(handle, "\""));
    }
}
