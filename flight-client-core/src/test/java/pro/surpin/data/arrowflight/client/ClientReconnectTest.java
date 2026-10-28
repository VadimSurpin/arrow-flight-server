package pro.surpin.data.arrowflight.client;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ClientReconnectTest {

    /** Answers every getInfo with an empty plan and counts the calls. */
    private static final class Producer extends NoOpFlightProducer {
        private final AtomicInteger calls = new AtomicInteger();
        private final int failFirst;

        Producer(int failFirst) {
            this.failFirst = failFirst;
        }

        @Override
        public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
            if (calls.incrementAndGet() <= failFirst) {
                throw CallStatus.UNAVAILABLE.withDescription("starting").toRuntimeException();
            }
            return new FlightInfo(new Schema(Collections.emptyList()), descriptor,
                    Collections.emptyList(), 0, 0);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static FlightServer start(BufferAllocator allocator, int port, Producer producer) throws Exception {
        return FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", port), producer)
                .build().start();
    }

    private static Configuration config(int port, String token) {
        Configuration config = new Configuration("localhost", port, "user", null, token);
        config.setRetryBackoffMs(50);
        config.setMaxRetries(5);
        return config;
    }

    @Test
    void retriesAnUnavailableAnswerFromTheServer() throws Exception {
        int port = freePort();
        try (BufferAllocator allocator = new RootAllocator();
             FlightServer server = start(allocator, port, new Producer(2))) {
            Client client = Client.getOrCreate(config(port, "retry"));
            try {
                assertNotNull(client.getQueryEndpoints("select 1"));
            } finally {
                client.close();
            }
        }
    }

    @Test
    void keepsWorkingAfterTheServerRestartsOnTheSamePort() throws Exception {
        int port = freePort();
        Producer producer = new Producer(0);
        Client client;
        try (BufferAllocator allocator = new RootAllocator()) {
            FlightServer first = start(allocator, port, producer);
            client = Client.getOrCreate(config(port, "restart"));
            try {
                assertNotNull(client.getQueryEndpoints("select 1"));
                first.close();
                try (FlightServer second = start(allocator, port, producer)) {
                    assertNotNull(client.getQueryEndpoints("select 1"));
                    assertEquals(2, producer.calls.get());
                }
            } finally {
                client.close();
            }
        }
    }
}
