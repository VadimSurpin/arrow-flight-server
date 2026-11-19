package pro.surpin.data.arrowflight.server.e2e;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

import java.nio.file.Path;

/**
 * Starts the Flight SQL server over a directory of Parquet tables and blocks until killed, for end-to-end
 * runs against an external StarRocks. Prints the chosen port as {@code FLIGHT_PORT=<port>}.
 */
public final class StandaloneTestFlightServer {
    private StandaloneTestFlightServer() { }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(args.length > 0 ? args[0] : "src/test/resources/test_db").toAbsolutePath();
        try (TestFlightServerHelper flight = TestFlightServerHelper.builder().dataDir(dataDir.toString()).start()) {
            System.out.println("FLIGHT_PORT=" + flight.server.getPort());
            System.out.flush();
            Thread.currentThread().join();
        }
    }
}
