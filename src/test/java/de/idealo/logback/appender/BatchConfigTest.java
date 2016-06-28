package de.idealo.logback.appender;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class BatchConfigTest {

    private BatchConfig batchConfig;

    @Test
    public void batchIsFinishedBecauseOfMaxItems() throws Exception {
        batchConfig = new BatchConfig(2, 100);
        assertThat(batchConfig.isBatchFinished(), is(false));

        batchConfig.addBatchItem();
        assertThat(batchConfig.isBatchFinished(), is(false));

        batchConfig.addBatchItem();
        assertThat(batchConfig.isBatchFinished(), is(true));

        // new batch
        batchConfig.addBatchItem();
        assertThat(batchConfig.isBatchFinished(), is(false));

        batchConfig.addBatchItem();
        assertThat(batchConfig.isBatchFinished(), is(true));
    }

    @Test
    public void batchIsFinishedBecauseOfMaxTime() throws Exception {
        batchConfig = new BatchConfig(100, 1);
        assertThat(batchConfig.isBatchFinished(), is(false));
        batchConfig.addBatchItem();

        Thread.sleep(10L);
        assertThat(batchConfig.isBatchFinished(), is(false));

        Thread.sleep(1500L);
        assertThat(batchConfig.isBatchFinished(), is(true));
    }
}