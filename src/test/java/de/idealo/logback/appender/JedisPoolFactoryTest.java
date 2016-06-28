package de.idealo.logback.appender;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

public class JedisPoolFactoryTest {

    private JedisPoolFactory jedisPoolFactory;

    private RedisConnectionConfig redisConnectionConfigMock;
    private JedisPool jedisPoolMock;
    private JedisSentinelPool jedisSentinelPoolMock;

    @Before
    public void setUp() throws Exception {
        redisConnectionConfigMock = mock(RedisConnectionConfig.class);
        when(redisConnectionConfigMock.getDatabase()).thenReturn(0);
        when(redisConnectionConfigMock.getKey()).thenReturn("dummyKey");
        when(redisConnectionConfigMock.getPassword()).thenReturn("secret");

        jedisPoolMock = mock(JedisPool.class);
        jedisSentinelPoolMock = mock(JedisSentinelPool.class);

        jedisPoolFactory = Mockito.spy(new JedisPoolFactory(redisConnectionConfigMock));
        doReturn(jedisPoolMock).when(jedisPoolFactory).createJedisPool();
        doReturn(jedisSentinelPoolMock).when(jedisPoolFactory).createJedisSentinelPool();
    }

    @Test
    public void aJedisPoolIsCreated() throws Exception {
        when(redisConnectionConfigMock.getScheme()).thenReturn(RedisConnectionConfig.RedisScheme.NODE);
        when(redisConnectionConfigMock.getHost()).thenReturn("om-test-02");
        when(redisConnectionConfigMock.getPort()).thenReturn(6379);

        final Pool<Jedis> pool = jedisPoolFactory.createPool();

        assertThat(pool, is(instanceOf(JedisPool.class)));
    }

    @Test
    public void aJedisSentinelPoolIsCreated() throws Exception {
        when(redisConnectionConfigMock.getScheme()).thenReturn(RedisConnectionConfig.RedisScheme.SENTINEL);
        when(redisConnectionConfigMock.getSentinels()).thenReturn("om-test-02:6379, om-test-03:6379");
        when(redisConnectionConfigMock.getSentinelMasterName()).thenReturn("mymaster");

        final Pool<Jedis> pool = jedisPoolFactory.createPool();

        assertThat(pool, is(instanceOf(JedisSentinelPool.class)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void anExceptionIsThrownDuringJedisPoolCreationWithNullScheme() throws Exception {
        when(redisConnectionConfigMock.getScheme()).thenReturn(null);
        when(redisConnectionConfigMock.getHost()).thenReturn("om-test-02");
        when(redisConnectionConfigMock.getPort()).thenReturn(6379);

        final Pool<Jedis> pool = jedisPoolFactory.createPool();

        assertThat(pool, is(instanceOf(JedisPool.class)));
    }

    @Test
    public void sentinelsCanBeExtracted() throws Exception {
        final Set<String> sentinels = jedisPoolFactory.getSentinels("om-test-02:6379, om-test-03:6379");

        assertThat(sentinels, containsInAnyOrder("om-test-02:6379", "om-test-03:6379"));
    }
}