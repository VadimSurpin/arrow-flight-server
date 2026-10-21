package pro.surpin.data.arrowflight.server;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.FieldVector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.services.RuntimeInFilters;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A runtime-filter block appended to a real ticket narrows the scan over a real Flight connection. */
@Tag("integration")
class RuntimeFilterIntegrationTest {

    private static final String QUERY = "SELECT bigint_col, tinyint_col FROM test_schema.test_table";

    private static TestFlightServerHelper helper;
    private static FlightSqlClient sqlClient;

    @BeforeAll
    static void startAll() throws Exception {
        helper = TestFlightServerHelper.builder().start();
        sqlClient = helper.sqlClient();
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    private static Ticket withBlock(Ticket ticket, byte[] block) throws Exception {
        Any any = Any.parseFrom(ticket.getBytes());
        return new Ticket(any.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(15, UnknownFieldSet.Field.newBuilder()
                        .addLengthDelimited(ByteString.copyFrom(block)).build()).build()).build().toByteArray());
    }

    /** Rows of both columns as "bigint,tinyint", read from every endpoint of the query. */
    private static List<String> rows(byte[] block) throws Exception {
        FlightInfo info = sqlClient.execute(QUERY);
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            Ticket ticket = block == null ? endpoint.getTicket() : withBlock(endpoint.getTicket(), block);
            try (FlightStream stream = helper.flightClient().getStream(ticket)) {
                while (stream.next()) {
                    FieldVector first = stream.getRoot().getVector("bigint_col");
                    FieldVector second = stream.getRoot().getVector("tinyint_col");
                    for (int i = 0; i < stream.getRoot().getRowCount(); i++) {
                        rows.add(first.getObject(i) + "," + second.getObject(i));
                    }
                }
            }
        }
        return rows;
    }

    private static long[] firstColumn(List<String> rows, int limit) {
        return new TreeSet<>(rows.stream().map(r -> Long.parseLong(r.split(",")[0])).toList())
                .stream().limit(limit).mapToLong(Long::longValue).toArray();
    }

    @Test
    void filteredScanReturnsExactlyTheMatchingRows() throws Exception {
        List<String> all = rows(null);
        long[] keep = firstColumn(all, 3);
        assertTrue(keep.length > 0 && all.size() > keep.length);

        byte[] block = RuntimeInFilters.encode(List.of(new RuntimeInFilters.Filter("bigint_col", keep)));
        List<String> filtered = rows(block);

        List<String> expected = all.stream()
                .filter(r -> java.util.Arrays.stream(keep).anyMatch(k -> k == Long.parseLong(r.split(",")[0])))
                .toList();
        assertEquals(expected.stream().sorted().toList(), filtered.stream().sorted().toList());
        assertTrue(filtered.size() < all.size());
    }

    @Test
    void filterOnAColumnTheQueryDoesNotReturnChangesNothing() throws Exception {
        List<String> all = rows(null);
        byte[] block = RuntimeInFilters.encode(List.of(new RuntimeInFilters.Filter("not_selected", new long[]{1})));
        assertEquals(all.stream().sorted().toList(), rows(block).stream().sorted().toList());
    }

    @Test
    void anEmptyFilterReturnsNoRows() throws Exception {
        byte[] block = RuntimeInFilters.encode(List.of(new RuntimeInFilters.Filter("bigint_col", new long[]{})));
        assertEquals(0, rows(block).size());
    }
}
