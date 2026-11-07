package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class S3SettingsTest {

    private static S3Settings from(Map<String, String> env) {
        return S3Settings.fromLookup(env::get);
    }

    @Test
    void isRemoteForS3AndS3Schemes() {
        assertTrue(S3Settings.isRemote("s3a://tpch"));
        assertTrue(S3Settings.isRemote("s3://tpch/sf100"));
        assertFalse(S3Settings.isRemote("/data/parquet"));
        assertFalse(S3Settings.isRemote("hdfs://nn/data"));
        assertFalse(S3Settings.isRemote(null));
    }

    @Test
    void endpointWithSchemeSplitsIntoHostAndSsl() {
        S3Settings s = from(Map.of("FLIGHT_S3_ENDPOINT", "http://minio:9000"));
        assertEquals("minio:9000", s.endpointHost());
        assertFalse(s.useSsl());
        assertTrue(s.pathStyle());

        S3Settings secure = from(Map.of("FLIGHT_S3_ENDPOINT", "https://s3.example.org"));
        assertEquals("s3.example.org", secure.endpointHost());
        assertTrue(secure.useSsl());
    }

    @Test
    void hadoopConfigurationCarriesEndpointCredentialsAndPathStyle() {
        S3Settings s = from(Map.of(
                "FLIGHT_S3_ENDPOINT", "http://minio:9000",
                "FLIGHT_S3_ACCESS_KEY", "ak",
                "FLIGHT_S3_SECRET_KEY", "sk",
                "FLIGHT_S3_REGION", "eu-west-1"));
        Configuration conf = new Configuration(false);
        s.applyTo(conf);
        assertEquals("http://minio:9000", conf.get("fs.s3a.endpoint"));
        assertEquals("ak", conf.get("fs.s3a.access.key"));
        assertEquals("sk", conf.get("fs.s3a.secret.key"));
        assertEquals("eu-west-1", conf.get("fs.s3a.endpoint.region"));
        assertTrue(conf.getBoolean("fs.s3a.path.style.access", false));
        assertFalse(conf.getBoolean("fs.s3a.connection.ssl.enabled", true));
    }

    @Test
    void unsetValuesLeaveHadoopConfigurationAlone() {
        Configuration conf = new Configuration(false);
        from(Map.of()).applyTo(conf);
        assertNull(conf.get("fs.s3a.endpoint"));
        assertNull(conf.get("fs.s3a.access.key"));
    }

    @Test
    void duckDbStatementsLoadHttpfsAndSetEndpoint() {
        S3Settings s = from(Map.of(
                "FLIGHT_S3_ENDPOINT", "http://minio:9000",
                "FLIGHT_S3_ACCESS_KEY", "a'k",
                "FLIGHT_S3_SECRET_KEY", "sk"));
        List<String> sql = s.duckDbStatements();
        assertEquals("INSTALL httpfs", sql.get(0));
        assertEquals("LOAD httpfs", sql.get(1));
        assertTrue(sql.contains("SET s3_endpoint = 'minio:9000'"));
        assertTrue(sql.contains("SET s3_use_ssl = false"));
        assertTrue(sql.contains("SET s3_url_style = 'path'"));
        assertTrue(sql.contains("SET s3_access_key_id = 'a''k'"), "quotes in credentials are escaped");
        assertTrue(sql.contains("SET s3_secret_access_key = 'sk'"));
    }

    @Test
    void duckDbPathMapsS3aToS3() {
        assertEquals("s3://tpch/sf100/x.parquet", S3Settings.duckDbPath("s3a://tpch/sf100/x.parquet"));
        assertEquals("s3://tpch/x", S3Settings.duckDbPath("s3n://tpch/x"));
        assertEquals("s3://tpch/x", S3Settings.duckDbPath("s3://tpch/x"));
        assertEquals("/data/x.parquet", S3Settings.duckDbPath("/data/x.parquet"));
    }
}
