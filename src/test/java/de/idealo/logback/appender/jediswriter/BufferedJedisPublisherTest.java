package de.idealo.logback.appender.jediswriter;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import de.idealo.logback.appender.jedisclient.JedisClient;

import ch.qos.logback.core.spi.DeferredProcessingAware;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class BufferedJedisPublisherTest {
    private static final String KEY = "TEST_KEY";
    private static final int DEFAULT_BUFFER_ITEMS = 3;
    private static final int DEFAULT_BATCH_WAIT_MILLIS = 100;

    @Mock
    private JedisClient client;
    @Mock
    private Function<DeferredProcessingAware, String> messageCreator;
    @Mock
    private Pipeline pipeline;

    private AbstractBufferedJedisWriter writer;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(messageCreator.apply(Matchers.any())).thenAnswer(invocation -> String.valueOf(invocation.getArgumentAt(0, DeferredProcessingAware.class)));
        final Optional<Pipeline> defaultPipeline = Optional.of(pipeline);
        when(client.getPipeline()).thenReturn(defaultPipeline);

        writer = new BufferedJedisPublisher(client, messageCreator, KEY, DEFAULT_BUFFER_ITEMS, DEFAULT_BATCH_WAIT_MILLIS);
    }

    @After
    public void shutdown() {
        writer.close();
    }

    @Test
    public void flusher_thread_is_running() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(Math.round(DEFAULT_BATCH_WAIT_MILLIS * 2.5));
        Assert.assertEquals(2, writer.getFlusherThreadActions());
    }

    @Test
    public void ignore_null_events() throws InterruptedException {
        for (int i = 0; i < DEFAULT_BUFFER_ITEMS; i++) {
            writer.append(null);
        }
        verify(pipeline, times(0)).publish(anyString(), anyString());
        verify(pipeline, times(0)).sync();
    }

    @Test
    public void send_on_full_queue() throws InterruptedException {
        int batchFullEvents = 5;
        for (int i = 0; i < DEFAULT_BUFFER_ITEMS * batchFullEvents; i++) {
            writer.append(mock(DeferredProcessingAware.class));
        }
        final int expectedPublishCalls = DEFAULT_BUFFER_ITEMS * batchFullEvents; // one per item
        verify(pipeline, times(expectedPublishCalls)).publish(anyString(), anyString());
        verify(pipeline, times(batchFullEvents)).sync();
    }

    @Test
    public void send_on_second_try_due_to_no_pipline_on_first_try() throws InterruptedException {
        when(client.getPipeline()).thenReturn(Optional.empty()).thenReturn(Optional.of(pipeline));
        for (int i = 0; i < DEFAULT_BUFFER_ITEMS; i++) {
            writer.append(mock(DeferredProcessingAware.class));
        }
        verify(client, times(2)).getPipeline();
        verify(client).reconnect();
        verify(pipeline, times(DEFAULT_BUFFER_ITEMS)).publish(anyString(), anyString());
        verify(pipeline, times(1)).sync();
    }

    @Test
    public void send_on_second_try_due_to_exception_on_first_publish() throws InterruptedException {
        when(pipeline.publish(anyString(), anyString())).thenThrow(new JedisConnectionException("")).thenReturn(null);
        for (int i = 0; i < DEFAULT_BUFFER_ITEMS; i++) {
            writer.append(mock(DeferredProcessingAware.class));
        }
        final int expectedPublishCalls = 1 + DEFAULT_BUFFER_ITEMS; // one failed, then for each buffer item
        verify(client, times(2)).getPipeline();
        verify(client).reconnect();
        verify(pipeline, times(expectedPublishCalls)).publish(anyString(), anyString());
        verify(pipeline, times(1)).sync();
    }

    @Test
    public void dont_send_on_too_many_failures() throws InterruptedException {
        when(pipeline.publish(anyString(), anyString())).thenThrow(new JedisConnectionException(""));
        for (int i = 0; i < DEFAULT_BUFFER_ITEMS; i++) {
            writer.append(mock(DeferredProcessingAware.class));
        }
        verify(client, times(2)).getPipeline();
        verify(client, times(2)).reconnect();
        verify(pipeline, times(2)).publish(anyString(), anyString());
        verify(pipeline, times(0)).sync();
    }
}
