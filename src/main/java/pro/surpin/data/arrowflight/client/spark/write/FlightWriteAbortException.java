package pro.surpin.data.arrowflight.client.spark.write;

import java.io.IOException;
import java.io.Serializable;

/**
 * Exception thrown when a streaming write is aborted, carrying partition, task, and message count details.
 */
public class FlightWriteAbortException extends IOException implements Serializable {
    /**
     * Construct a success Flight-Writer-Commit-Message
     * @param partitionId - the partition-id of the data-frame been written
     * @param taskId - the task id of the writing operation
     * @param messageCount - number of rows been written
     */
    public FlightWriteAbortException(int partitionId, long taskId, long messageCount) {
        super(getMessage(partitionId, taskId, messageCount));
    }

    /**
     * Construct a failure Flight-Writer-Commit-Message
     * @param partitionId - the partition-id of the data-frame been written
     * @param taskId - the task id of the writing operation
     * @param epochId - the epoch-id for streaming write.
     * @param messageCount - number of rows been written
     */
    public FlightWriteAbortException(int partitionId, long taskId, String epochId, long messageCount) {
        super(getMessage(partitionId, taskId, epochId, messageCount));
    }

    /**
     * @param partitionId partition id of the data frame
     * @param taskId task id of the write operation
     * @param messageCount number of rows written
     * @return formatted abort message
     */
    private static String getMessage(int partitionId, long taskId, long messageCount) {
        return String.format("Streaming write for %d messages with partition (%d), task (%d) aborted.", messageCount, partitionId, taskId);
    }

    /**
     * @param partitionId partition id of the data frame
     * @param taskId task id of the write operation
     * @param epochId epoch id for streaming write
     * @param messageCount number of rows written
     * @return formatted abort message with epoch info
     */
    private static String getMessage(int partitionId, long taskId, String epochId, long messageCount) {
        return String.format("Streaming write for %d messages with partition (%d), task (%d) and epoch (%s) aborted.", messageCount, partitionId, taskId, epochId);
    }
}
