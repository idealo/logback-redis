package de.idealo.logback.appender.jedisclient;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.util.Pool;

public class JedisPoolFactoryTest {

    @Mock
    private RedisConnectionConfig redisConnectionConfig;

    @Mock
    private JedisSentinelPool jedisSentinelPool;
    @Mock
    private JedisPool jedisPool;

    @Mock
    private JedisPoolCreator jedisPoolCreator;
    @InjectMocks
    private JedisPoolFactory jedisPoolFactory;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(jedisPoolCreator.createJedisPool(Matchers.any())).thenReturn(jedisPool);
        when(jedisPoolCreator.createJedisSentinelPool(Matchers.any())).thenReturn(jedisSentinelPool);
    }

    @Test
    public void create_jedis_pool_on_node_scheme() throws Exception {

        when(redisConnectionConfig.getScheme()).thenReturn(RedisConnectionConfig.RedisScheme.NODE);

        final Pool<Jedis> pool = jedisPoolFactory.createPool(redisConnectionConfig);

        assertThat(pool, is(instanceOf(JedisPool.class)));
    }

    @Test
    public void create_sentinel_pool_on_sentinel_scheme() throws Exception {

        when(redisConnectionConfig.getScheme()).thenReturn(RedisConnectionConfig.RedisScheme.SENTINEL);

        final Pool<Jedis> pool = jedisPoolFactory.createPool(redisConnectionConfig);

        assertThat(pool, is(instanceOf(JedisSentinelPool.class)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void exception_on_null_scheme() throws Exception {

        when(redisConnectionConfig.getScheme()).thenReturn(null);

        final Pool<Jedis> pool = jedisPoolFactory.createPool(redisConnectionConfig);

        assertThat(pool, is(instanceOf(JedisPool.class)));
    }
}
