package pro.surpin.data.arrowflight.server.adapters;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Runs bounded parallel reads of Parquet footers for planning and statistics. */
public final class ParquetFooterExecutor {
    private static final ExecutorService POOL = Executors.newFixedThreadPool(
            Integer.getInteger("parquetFooterThreads", 16), runnable -> {
                Thread thread = new Thread(runnable, "parquet-footer");
                thread.setDaemon(true);
                return thread;
            });

    private ParquetFooterExecutor() { }

    /**
     * Submits one footer read to the shared pool.
     *
     * @param task footer read
     * @param <T> result type
     * @return task result
     */
    public static <T> Future<T> submit(Callable<T> task) {
        return POOL.submit(task);
    }
}
