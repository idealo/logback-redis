package de.idealo.logback.appender;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.DeferredProcessingAware;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class BufferedJedisWriterTest {
    private static final String KEY = "TEST_KEY";
    private static final byte[] EMPTY_MESSAGE = new byte[0];
    private static final int DEFAULT_BATCH_ITEMS = 3;
    private static final int DEFAULT_BATCH_WAIT_MILLIS = 100;

    @Mock
    private JedisClient client;
    @Mock
    private Encoder<DeferredProcessingAware> encoder;
    @Mock
    private Pipeline pipeline;

    private BufferedJedisWriter writer;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(encoder.encode(Matchers.any())).thenReturn(EMPTY_MESSAGE);
        final Optional<Pipeline> defaultPipeline = Optional.of(pipeline);
        when(client.getPipeline()).thenReturn(defaultPipeline);

        writer = new BufferedJedisWriter(client, encoder, KEY, DEFAULT_BATCH_ITEMS, DEFAULT_BATCH_WAIT_MILLIS);
    }

    @After
    public void shutdown() {
        writer.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void exceptionOnMissingEncoder() {
        try (BufferedJedisWriter noInstance = new BufferedJedisWriter(client, null, KEY, 1, 1)) {
        }
    }

    @Test
    public void testRunningFlusherThread() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(Math.round(DEFAULT_BATCH_WAIT_MILLIS * 2.5));
        Assert.assertEquals(2, writer.getFlusherThreadActions());
    }

    @Test
    public void testIgnoreNullItems() throws InterruptedException {
        for (int i = 0; i < DEFAULT_BATCH_ITEMS; i++) {
            writer.append(null);
        }
        verify(pipeline, times(0)).rpush(anyString(), anyVararg());
        verify(pipeline, times(0)).sync();
    }

    @Test
    public void testPushOnFullBatch() throws InterruptedException {
        int batchFullEvents = 5;
        for (int i = 0; i < DEFAULT_BATCH_ITEMS * batchFullEvents; i++) {
            writer.append(mock(DeferredProcessingAware.class));
        }
        verify(pipeline, times(batchFullEvents)).rpush(anyString(), anyVararg());
        verify(pipeline, times(batchFullEvents)).sync();
    }

    @Test
    public void testPushOnSecondTryNoPipeline() throws InterruptedException {
        when(client.getPipeline()).thenReturn(Optional.empty()).thenReturn(Optional.of(pipeline));
        for (int i = 0; i < DEFAULT_BATCH_ITEMS; i++) {
            writer.append(mock(DeferredProcessingAware.class));
        }
        verify(client, times(2)).getPipeline();
        verify(client).reconnect();
        verify(pipeline, times(1)).rpush(anyString(), anyVararg());
        verify(pipeline, times(1)).sync();
    }

    @Test
    public void testPushOnSecondTryErrorPush() throws InterruptedException {
        when(pipeline.rpush(anyString(), anyVararg())).thenThrow(new JedisConnectionException("")).thenReturn(null);
        for (int i = 0; i < DEFAULT_BATCH_ITEMS; i++) {
            writer.append(mock(DeferredProcessingAware.class));
        }
        verify(client, times(2)).getPipeline();
        verify(client).reconnect();
        verify(pipeline, times(2)).rpush(anyString(), anyVararg());
        verify(pipeline, times(1)).sync();
    }

    @Test
    public void testPushFailed() throws InterruptedException {
        when(pipeline.rpush(anyString(), anyVararg())).thenThrow(new JedisConnectionException(""));
        for (int i = 0; i < DEFAULT_BATCH_ITEMS; i++) {
            writer.append(mock(DeferredProcessingAware.class));
        }
        verify(client, times(2)).getPipeline();
        verify(client, times(2)).reconnect();
        verify(pipeline, times(2)).rpush(anyString(), anyVararg());
        verify(pipeline, times(0)).sync();
    }
}
