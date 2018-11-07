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
    public void testEmptyOptionalOnPoolCreationError() {
        when(poolFactory.createPool(Matchers.any())).thenThrow(new RuntimeException());
        assertEquals(null, clientProvider.getJedisClient().orElse(null));
    }

    @Test
    public void testEmptyOptionalOngetResourceError() {
        when(resultPool.getResource()).thenThrow(new RuntimeException());

        assertEquals(null, clientProvider.getJedisClient().orElse(null));
    }

    @Test
    public void testValidResultOnNoExceptions() {
        assertEquals(client, clientProvider.getJedisClient().orElse(null));
    }

    @Test
    public void testValidResultOnSecondTry() {
        when(resultPool.getResource()).thenThrow(new RuntimeException()).thenReturn(client);
        assertEquals(null, clientProvider.getJedisClient().orElse(null));
        assertEquals(client, clientProvider.getJedisClient().orElse(null));
    }
}
