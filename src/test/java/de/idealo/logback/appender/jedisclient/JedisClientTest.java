package de.idealo.logback.appender.jedisclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

import de.idealo.logback.appender.jedisclient.JedisClient;
import de.idealo.logback.appender.jedisclient.JedisClientProvider;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

public class JedisClientTest {

    private static final int MAX_INIT_RETRIES = 3;
    private static final long INIT_RETRIES_INTERVAL_MILLIS = 100L;
    @Mock
    private JedisClientProvider clientProvider;
    @Mock
    private Jedis jedis;
    @Mock
    private Pipeline pipeline;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        when(clientProvider.getJedisClient()).thenReturn(Optional.of(jedis));
        when(jedis.pipelined()).thenReturn(pipeline);
    }

    @Test(expected = IllegalArgumentException.class)
    public void exception_on_invalid_max_tries_value() {
        try (JedisClient jedisClient = new JedisClient(clientProvider, 0, INIT_RETRIES_INTERVAL_MILLIS)) {

        }
    }

    @Test
    public void valid_client_on_last_retry() throws InterruptedException {
        withClientOnTryNumber(MAX_INIT_RETRIES);
        try (JedisClient jedisClient = new JedisClient(clientProvider, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            TimeUnit.MILLISECONDS.sleep(INIT_RETRIES_INTERVAL_MILLIS * (MAX_INIT_RETRIES + 1));

            verify(clientProvider, times(MAX_INIT_RETRIES)).getJedisClient();
            assertEquals(pipeline, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void no_client_on_exceeded_retries() throws InterruptedException {
        int clientOnTry = MAX_INIT_RETRIES + 1;
        withClientOnTryNumber(clientOnTry);
        try (JedisClient jedisClient = new JedisClient(clientProvider, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            TimeUnit.MILLISECONDS.sleep(INIT_RETRIES_INTERVAL_MILLIS * (MAX_INIT_RETRIES + 1));

            verify(clientProvider, times(MAX_INIT_RETRIES)).getJedisClient();
            assertEquals(null, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void reconnect_succeeds_after_init_failed() throws InterruptedException {
        int clientOnTry = MAX_INIT_RETRIES + 1;
        withClientOnTryNumber(clientOnTry);
        try (JedisClient jedisClient = new JedisClient(clientProvider, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            TimeUnit.MILLISECONDS.sleep(INIT_RETRIES_INTERVAL_MILLIS * (MAX_INIT_RETRIES + 1));

            verify(clientProvider, times(MAX_INIT_RETRIES)).getJedisClient();
            assertEquals(null, jedisClient.getPipeline().orElse(null));

            jedisClient.reconnect();
            verify(clientProvider, times(MAX_INIT_RETRIES + 1)).getJedisClient();
            assertEquals(pipeline, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void no_pipeline_on_failed_reconnect() throws InterruptedException {
        when(clientProvider.getJedisClient()).thenReturn(Optional.of(jedis)).thenReturn(Optional.empty());
        doThrow(new JedisException("")).when(jedis).close();

        try (JedisClient jedisClient = new JedisClient(clientProvider, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            verify(clientProvider, times(1)).getJedisClient();
            assertEquals(pipeline, jedisClient.getPipeline().orElse(null));

            jedisClient.reconnect();

            verify(clientProvider, times(2)).getJedisClient();
            assertEquals(null, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void new_pipeline_on_successful_reconnect() throws InterruptedException {
        Jedis newJedis = mock(Jedis.class);
        Pipeline newPipeline = mock(Pipeline.class);
        when(newJedis.pipelined()).thenReturn(newPipeline);

        when(clientProvider.getJedisClient()).thenReturn(Optional.of(jedis)).thenReturn(Optional.of(newJedis));
        try (JedisClient jedisClient = new JedisClient(clientProvider, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {
            verify(clientProvider, times(1)).getJedisClient();
            assertEquals(pipeline, jedisClient.getPipeline().orElse(null));

            jedisClient.reconnect();

            verify(clientProvider, times(2)).getJedisClient();
            assertEquals(newPipeline, jedisClient.getPipeline().orElse(null));
        }
    }

    @Test
    public void no_reconnect_during_initialization() throws InterruptedException {
        final Jedis firstInitTryResult = mock(Jedis.class);
        final Jedis reconnectResult = mock(Jedis.class);

        final Pipeline reconnectPipeline = mock(Pipeline.class);
        when(reconnectResult.pipelined()).thenReturn(reconnectPipeline);

        when(clientProvider.getJedisClient()).thenReturn(Optional.empty())
                .thenReturn(Optional.of(firstInitTryResult))
                .thenReturn(Optional.of(reconnectResult));
        try (JedisClient jedisClient = new JedisClient(clientProvider, MAX_INIT_RETRIES, INIT_RETRIES_INTERVAL_MILLIS)) {

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
    public void stop_initialization_on_shutdown() throws InterruptedException {
        final int initRetryMillis = 50;
        when(clientProvider.getJedisClient()).thenReturn(Optional.empty());
        try (JedisClient jedisClient = new JedisClient(clientProvider, Integer.MAX_VALUE, initRetryMillis)) {

            assertTrue(jedisClient.isInitializing());

            TimeUnit.MILLISECONDS.sleep(3 * initRetryMillis);
            assertTrue(jedisClient.isInitializing());

            jedisClient.close();

            TimeUnit.MILLISECONDS.sleep(initRetryMillis);
            assertFalse(jedisClient.isInitializing());
        }
    }

    private void withClientOnTryNumber(int tries) {
        OngoingStubbing<Optional<Jedis>> stub = when(clientProvider.getJedisClient());
        for (int i = 1; i < tries; i++) {
            stub = stub.thenReturn(Optional.empty());
        }
        stub.thenReturn(Optional.of(jedis));
    }
}
