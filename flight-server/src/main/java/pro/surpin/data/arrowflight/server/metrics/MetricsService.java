package pro.surpin.data.arrowflight.server.metrics;

import com.sun.management.UnixOperatingSystemMXBean;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import pro.surpin.data.arrowflight.server.model.ExecutionPath;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Exposes low-overhead Prometheus metrics for the Flight server.
 */
public final class MetricsService implements AutoCloseable {

    private static final String METRIC_TYPE_GAUGE = "gauge";
    private static final String METRIC_TYPE_COUNTER = "counter";
    private static final double[] DURATION_BUCKETS = {
        0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0
    };
    private static final ConcurrentHashMap<String, QueryMetrics> QUERY_METRICS =
            new ConcurrentHashMap<>();
    private static final AtomicLong ACTIVE_QUERIES = new AtomicLong();

    private final HttpServer server;
    private final ExecutorService executor;

    /**
     * Creates a metrics endpoint bound to all network interfaces.
     *
     * @param port Prometheus HTTP port, or zero for an ephemeral port
     * @throws IOException if the endpoint cannot be created
     */
    public MetricsService(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "arrowflight-metrics");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.createContext("/metrics", MetricsService::handleMetrics);
        server.createContext("/-/healthy", MetricsService::handleHealth);
    }

    /**
     * Starts accepting Prometheus scrape requests.
     */
    public void start() {
        server.start();
    }

    /**
     * Returns the bound HTTP port.
     *
     * @return metrics HTTP port
     */
    public int port() {
        return server.getAddress().getPort();
    }

    /**
     * Starts observing one logical Parquet query.
     *
     * @param logicalBytes planned Parquet input bytes
     * @return observation that must be closed when execution finishes
     */
    public static QueryObservation observeQuery(long logicalBytes) {
        ACTIVE_QUERIES.incrementAndGet();
        return new QueryObservation(Math.max(0L, logicalBytes));
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    /**
     * Handles a Prometheus scrape request.
     *
     * @param exchange HTTP exchange
     * @throws IOException if the response cannot be sent
     */
    private static void handleMetrics(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "method not allowed\n", "text/plain; charset=utf-8");
            return;
        }
        send(exchange, 200, render(),
                "text/plain; version=0.0.4; charset=utf-8");
    }

    /**
     * Handles a metrics health request.
     *
     * @param exchange HTTP exchange
     * @throws IOException if the response cannot be sent
     */
    private static void handleHealth(HttpExchange exchange) throws IOException {
        send(exchange, 200, "ok\n", "text/plain; charset=utf-8");
    }

    /**
     * Sends one HTTP response.
     *
     * @param exchange HTTP exchange
     * @param status HTTP status code
     * @param body response body
     * @param contentType response content type
     * @throws IOException if the response cannot be sent
     */
    private static void send(HttpExchange exchange, int status, String body,
            String contentType) throws IOException {
        byte[] content = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, content.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(content);
        }
    }

    /**
     * Renders the current registry in Prometheus text format.
     *
     * @return Prometheus text payload
     */
    private static String render() {
        StringBuilder metrics = new StringBuilder(8192);
        appendJvmMetrics(metrics);
        metric(metrics, "arrowflight_parquet_queries_active", METRIC_TYPE_GAUGE,
                "Currently executing Parquet queries", ACTIVE_QUERIES.get());
        appendQueryMetrics(metrics);

        return metrics.toString();
    }

    /**
     * Appends JVM and process metrics.
     *
     * @param metrics destination payload
     */
    private static void appendJvmMetrics(StringBuilder metrics) {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        metric(metrics, "arrowflight_jvm_heap_used_bytes", METRIC_TYPE_GAUGE,
                "Used JVM heap", memory.getHeapMemoryUsage().getUsed());
        metric(metrics, "arrowflight_jvm_heap_max_bytes", METRIC_TYPE_GAUGE,
                "Maximum JVM heap", memory.getHeapMemoryUsage().getMax());
        metric(metrics, "arrowflight_jvm_nonheap_used_bytes", METRIC_TYPE_GAUGE,
                "Used JVM non-heap memory", memory.getNonHeapMemoryUsage().getUsed());
        metric(metrics, "arrowflight_jvm_threads_live", METRIC_TYPE_GAUGE,
                "Live JVM threads", threads.getThreadCount());
        metric(metrics, "arrowflight_jvm_threads_daemon", METRIC_TYPE_GAUGE,
                "Live daemon JVM threads", threads.getDaemonThreadCount());
        metric(metrics, "arrowflight_jvm_threads_peak", METRIC_TYPE_GAUGE,
                "Peak live JVM threads", threads.getPeakThreadCount());
        java.lang.management.OperatingSystemMXBean operatingSystem =
                ManagementFactory.getOperatingSystemMXBean();
        metric(metrics, "arrowflight_process_cpu_available", METRIC_TYPE_GAUGE,
                "Processors available to the JVM", operatingSystem.getAvailableProcessors());
        metric(metrics, "arrowflight_system_load_average", METRIC_TYPE_GAUGE,
                "Operating system load average", operatingSystem.getSystemLoadAverage());
        if (operatingSystem instanceof com.sun.management.OperatingSystemMXBean extended) {
            metric(metrics, "arrowflight_process_cpu_time_seconds_total", METRIC_TYPE_COUNTER,
                    "CPU time used by the Flight JVM", seconds(extended.getProcessCpuTime()));
        }
        if (operatingSystem instanceof UnixOperatingSystemMXBean unix) {
            metric(metrics, "arrowflight_process_open_file_descriptors", METRIC_TYPE_GAUGE,
                    "Open file descriptors in the Flight JVM", unix.getOpenFileDescriptorCount());
        }
    }

    /**
     * Appends logical Parquet query metrics.
     *
     * @param metrics destination payload
     */
    private static void appendQueryMetrics(StringBuilder metrics) {
        helpType(metrics, "arrowflight_parquet_queries_total", METRIC_TYPE_COUNTER,
                "Completed logical Parquet queries");
        helpType(metrics, "arrowflight_parquet_query_failures_total", METRIC_TYPE_COUNTER,
                "Failed logical Parquet queries");
        helpType(metrics, "arrowflight_parquet_logical_input_bytes_total", METRIC_TYPE_COUNTER,
                "Planned logical Parquet input bytes");
        helpType(metrics, "arrowflight_parquet_query_duration_seconds", "histogram",
                "End-to-end Parquet scan and execution duration");
        QUERY_METRICS.forEach((path, values) -> {
            String labels = "{path=\"" + path + "\"}";
            sample(metrics, "arrowflight_parquet_queries_total" + labels,
                    values.count.get());
            sample(metrics, "arrowflight_parquet_query_failures_total" + labels,
                    values.failures.get());
            sample(metrics, "arrowflight_parquet_logical_input_bytes_total" + labels,
                    values.logicalBytes.get());
            for (int i = 0; i < DURATION_BUCKETS.length; i++) {
                String bucketLabels = "{path=\"" + path + "\",le=\""
                        + decimal(DURATION_BUCKETS[i]) + "\"}";
                sample(metrics, "arrowflight_parquet_query_duration_seconds_bucket"
                        + bucketLabels, values.durationBuckets.get(i));
            }
            sample(metrics, "arrowflight_parquet_query_duration_seconds_bucket{path=\""
                    + path + "\",le=\"+Inf\"}", values.count.get());
            sample(metrics, "arrowflight_parquet_query_duration_seconds_sum" + labels,
                    seconds(values.durationNanos.get()));
            sample(metrics, "arrowflight_parquet_query_duration_seconds_count" + labels,
                    values.count.get());
        });
    }

    /**
     * Appends one metric family containing a single sample.
     *
     * @param target destination payload
     * @param name metric name
     * @param type Prometheus metric type
     * @param help metric description
     * @param value sample value
     */
    private static void metric(StringBuilder target, String name, String type,
            String help, double value) {
        helpType(target, name, type, help);
        sample(target, name, value);
    }

    /**
     * Appends Prometheus help and type declarations.
     *
     * @param target destination payload
     * @param name metric name
     * @param type Prometheus metric type
     * @param help metric description
     */
    private static void helpType(StringBuilder target, String name, String type, String help) {
        target.append("# HELP ").append(name).append(' ').append(help).append('\n');
        target.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    /**
     * Appends one Prometheus sample.
     *
     * @param target destination payload
     * @param name metric name including optional labels
     * @param value sample value
     */
    private static void sample(StringBuilder target, String name, double value) {
        target.append(name).append(' ').append(decimal(value)).append('\n');
    }

    /**
     * Formats a floating-point value without locale-dependent separators.
     *
     * @param value sample value
     * @return Prometheus-compatible decimal
     */
    private static String decimal(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0 ? "+Inf" : "-Inf";
        }
        return String.format(Locale.ROOT, "%.9g", value);
    }

    /**
     * Converts nanoseconds to seconds.
     *
     * @param nanos duration in nanoseconds
     * @return duration in seconds
     */
    private static double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

    /**
     * Tracks one logical Parquet query until completion.
     */
    public static final class QueryObservation implements AutoCloseable {

        private final long logicalBytes;
        private final long startedNanos = System.nanoTime();
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile ExecutionPath path = ExecutionPath.UNKNOWN;
        private volatile boolean failed;

        /**
         * Creates an active query observation.
         *
         * @param logicalBytes planned Parquet input bytes
         */
        private QueryObservation(long logicalBytes) {
            this.logicalBytes = logicalBytes;
        }

        /**
         * Sets the execution path selected by the runtime.
         *
         * @param selectedPath selected execution path
         */
        public void executionPath(ExecutionPath selectedPath) {
            path = Objects.requireNonNull(selectedPath, "selectedPath");
        }

        /**
         * Marks this query as failed.
         */
        public void markFailed() {
            failed = true;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            long elapsedNanos = Math.max(0L, System.nanoTime() - startedNanos);
            QueryMetrics values = QUERY_METRICS.computeIfAbsent(path.label(),
                    ignored -> new QueryMetrics());
            values.count.incrementAndGet();
            values.logicalBytes.addAndGet(logicalBytes);
            values.durationNanos.addAndGet(elapsedNanos);
            if (failed) {
                values.failures.incrementAndGet();
            }
            double elapsedSeconds = seconds(elapsedNanos);
            for (int i = 0; i < DURATION_BUCKETS.length; i++) {
                if (elapsedSeconds <= DURATION_BUCKETS[i]) {
                    values.durationBuckets.incrementAndGet(i);
                }
            }
            ACTIVE_QUERIES.decrementAndGet();
        }
    }

    /**
     * Stores cumulative metrics for one bounded query path.
     */
    private static final class QueryMetrics {
        private final AtomicLong count = new AtomicLong();
        private final AtomicLong failures = new AtomicLong();
        private final AtomicLong logicalBytes = new AtomicLong();
        private final AtomicLong durationNanos = new AtomicLong();
        private final AtomicLongArray durationBuckets =
                new AtomicLongArray(DURATION_BUCKETS.length);
    }
}
