package pro.surpin.data.arrowflight.client.query;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;

class EndpointTest {

    @Test
    void toStringContainsUri() {
        URI[] uris = { URI.create("grpc://host:32010") };
        Endpoint ep = new Endpoint(uris, new byte[0]);

        String str = ep.toString();
        assertTrue(str.contains("grpc://host:32010"));
        assertTrue(str.contains("Endpoint"));
    }

}
