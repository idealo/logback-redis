package de.idealo.logback.appender;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class JedisClientProviderTest {
    @Mock
    private JedisPoolFactory poolFactory;
    @Mock
    private RedisConnectionConfig connectionConfig;
    @Mock
    private Pool<Jedis> resultPool;
    @Mock
    private Jedis client;

    @InjectMocks
    private JedisClientProvider clientProvider;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        when(poolFactory.createPool(Matchers.any())).thenReturn(resultPool);
        when(resultPool.getResource()).thenReturn(client);
    }

    @Test
    public void empty_optional_on_error_creating_pool() {
        when(poolFactory.createPool(Matchers.any())).thenThrow(new RuntimeException());
        assertEquals(null, clientProvider.getJedisClient().orElse(null));
    }

    @Test
    public void empty_optional_on_error_getting_resource() {
        when(resultPool.getResource()).thenThrow(new RuntimeException());

        assertEquals(null, clientProvider.getJedisClient().orElse(null));
    }

    @Test
    public void valid_client_on_no_exception() {
        assertEquals(client, clientProvider.getJedisClient().orElse(null));
    }

    @Test
    public void valid_client_on_second_try_due_to_error_getting_resource() {
        when(resultPool.getResource()).thenThrow(new RuntimeException()).thenReturn(client);
        assertEquals(null, clientProvider.getJedisClient().orElse(null));
        assertEquals(client, clientProvider.getJedisClient().orElse(null));
    }

    @Test
    public void valid_client_on_second_try_due_to_error_creating_pool() {
        when(poolFactory.createPool(Matchers.any())).thenThrow(new RuntimeException()).thenReturn(resultPool);
        assertEquals(null, clientProvider.getJedisClient().orElse(null));
        assertEquals(client, clientProvider.getJedisClient().orElse(null));
    }
}
