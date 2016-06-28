package de.idealo.logback.appender;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Configuration for the batches of log messages that are sent to redis.
 */
public class BatchConfig {

    private static final long MILLIS_PER_SECOND = 1000L;

    private static final long NO_BATCH_TIME_MARKER = -1L;

    private final int maxBatchItems;
    private final int maxBatchSeconds;

    private final LongAdder processedBatchItemsCount = new LongAdder();
    private final AtomicLong firstBatchItemTime = new AtomicLong(NO_BATCH_TIME_MARKER);

    public BatchConfig(int maxBatchItems, int maxBatchSeconds) {
        this.maxBatchItems = maxBatchItems;
        this.maxBatchSeconds = maxBatchSeconds;
    }

    public int getProcessedBatchItemsCount() {
        return processedBatchItemsCount.intValue();
    }

    public void addBatchItem() {
        processedBatchItemsCount.increment();
        firstBatchItemTime.compareAndSet(NO_BATCH_TIME_MARKER, System.currentTimeMillis());
    }

    public boolean isBatchFinished() {
        if (isBatchItemsLimitReached() || isBatchTimeLimitReached()) {
            reset();
            return true;
        }

        return false;
    }

    private boolean isBatchItemsLimitReached() {
        return processedBatchItemsCount.intValue() >= maxBatchItems;
    }

    private boolean isBatchTimeLimitReached() {
        return firstBatchItemTime.get() != NO_BATCH_TIME_MARKER && System.currentTimeMillis() - firstBatchItemTime.get() > maxBatchSeconds * MILLIS_PER_SECOND;
    }

    private void reset() {
        processedBatchItemsCount.reset();
        firstBatchItemTime.set(NO_BATCH_TIME_MARKER);
    }

    @Override
    public String toString() {
        return "BatchConfig{" +
                "maxBatchItems=" + maxBatchItems +
                ", maxBatchSeconds=" + maxBatchSeconds +
                ", processedBatchItemsCount=" + processedBatchItemsCount +
                ", firstBatchItemTime=" + firstBatchItemTime +
                '}';
    }
}
