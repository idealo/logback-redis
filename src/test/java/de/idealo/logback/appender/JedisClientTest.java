package de.idealo.logback.appender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

public class JedisClientTest {

    private static final int MAX_INIT_RETRIES = 3;
    private static final long INIT_RETRIES_INTERVAL_MILLIS = 100L;
    @Mock
    private Pool<Jedis> pool;
    @Mock
    private Jedis jedis;
    @Mock
    private Pipeline pipeline;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.pipelined()).thenReturn(pipeline);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionOnInvalidMaxTries() {
        try (JedisClient jedisClient = new JedisClient(pool, 0, INIT_RETRIES_INTERVAL_MILLIS)) {

        }
    }

    @Test
    public void testClientOnLastRetry() throws InterruptedException {
        withClientOnTryNumber(MAX_INIT_RETRIES);
        try (JedisClient jedisClient = new JedisClient(pool, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            TimeUnit.MILLISECONDS.sleep(INIT_RETRIES_INTERVAL_MILLIS * (MAX_INIT_RETRIES + 1));

            verify(pool, times(MAX_INIT_RETRIES)).getResource();
            assertEquals(pipeline, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void testNoClientOnMaxRetries() throws InterruptedException {
        int clientOnTry = MAX_INIT_RETRIES + 1;
        withClientOnTryNumber(clientOnTry);
        try (JedisClient jedisClient = new JedisClient(pool, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            TimeUnit.MILLISECONDS.sleep(INIT_RETRIES_INTERVAL_MILLIS * (MAX_INIT_RETRIES + 1));

            verify(pool, times(MAX_INIT_RETRIES)).getResource();
            assertEquals(null, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void testWorkingReconnectAfterInitFailed() throws InterruptedException {
        int clientOnTry = MAX_INIT_RETRIES + 1;
        withClientOnTryNumber(clientOnTry);
        try (JedisClient jedisClient = new JedisClient(pool, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            TimeUnit.MILLISECONDS.sleep(INIT_RETRIES_INTERVAL_MILLIS * (MAX_INIT_RETRIES + 1));

            verify(pool, times(MAX_INIT_RETRIES)).getResource();
            assertEquals(null, jedisClient.getPipeline().orElse(null));

            jedisClient.reconnect();
            verify(pool, times(MAX_INIT_RETRIES + 1)).getResource();
            assertEquals(pipeline, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void testNoPipelineOnFailingReconnect() throws InterruptedException {
        when(pool.getResource()).thenReturn(jedis).thenThrow(new RuntimeException());
        doThrow(new JedisException("")).when(jedis).close();

        try (JedisClient jedisClient = new JedisClient(pool, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            verify(pool, times(1)).getResource();
            assertEquals(pipeline, jedisClient.getPipeline().orElse(null));

            jedisClient.reconnect();

            verify(pool, times(2)).getResource();
            assertEquals(null, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void testNewPipelineOnSuccessfulReconnect() throws InterruptedException {
        Jedis newJedis = mock(Jedis.class);
        Pipeline newPipeline = mock(Pipeline.class);
        when(newJedis.pipelined()).thenReturn(newPipeline);

        when(pool.getResource()).thenReturn(jedis).thenReturn(newJedis);
        try (JedisClient jedisClient = new JedisClient(pool, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            verify(pool, times(1)).getResource();
            assertEquals(pipeline, jedisClient.getPipeline().orElse(null));

            jedisClient.reconnect();

            verify(pool, times(2)).getResource();
            assertEquals(newPipeline, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void testNoReconnectDuringInitialization() throws InterruptedException {
        final RuntimeException firstInitTryException = new RuntimeException();
        final Jedis firstInitTryResult = mock(Jedis.class);
        final Jedis reconnectResult = mock(Jedis.class);

        final Pipeline reconnectPipeline = mock(Pipeline.class);
        when(reconnectResult.pipelined()).thenReturn(reconnectPipeline);

        when(pool.getResource()).thenThrow(firstInitTryException).thenReturn(firstInitTryResult).thenReturn(reconnectResult);
        try (JedisClient jedisClient = new JedisClient(pool, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {

            jedisClient.reconnect(); // won't work, client is initializing
            assertEquals(null, jedisClient.getPipeline().orElse(null));
            TimeUnit.MILLISECONDS.sleep(INIT_RETRIES_INTERVAL_MILLIS * 2);
            // initialization resulted in firstInitTryResult
            jedisClient.reconnect();

            verify(firstInitTryResult).close();
            assertEquals(reconnectPipeline, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void testStopInitOnShutdown() throws InterruptedException {
        final int initRetryMillis = 50;
        when(pool.getResource()).thenThrow(new RuntimeException());
        try (JedisClient jedisClient = new JedisClient(pool, Integer.MAX_VALUE, initRetryMillis)) {

            assertTrue(jedisClient.isInitializing());

            TimeUnit.MILLISECONDS.sleep(3 * initRetryMillis);
            assertTrue(jedisClient.isInitializing());

            jedisClient.close();

            TimeUnit.MILLISECONDS.sleep(initRetryMillis);
            Assert.assertFalse(jedisClient.isInitializing());
        }
    }

    private void withClientOnTryNumber(int tries) {
        final RuntimeException runtimeException = new RuntimeException();
        OngoingStubbing<Jedis> stub = when(pool.getResource());
        for (int i = 1; i < tries; i++) {
            stub = stub.thenThrow(runtimeException);
        }
        stub.thenReturn(jedis);
    }
}
