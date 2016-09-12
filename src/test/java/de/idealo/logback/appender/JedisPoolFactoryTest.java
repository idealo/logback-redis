package de.idealo.logback.appender;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

@RunWith(MockitoJUnitRunner.class)
public class JedisPoolFactoryTest {

    @Spy
    @InjectMocks
    private JedisPoolFactory jedisPoolFactory;

    @Mock
    private RedisConnectionConfig redisConnectionConfigMock;
    @Mock
    private JedisPool jedisPoolMock;
    @Mock
    private JedisSentinelPool jedisSentinelPoolMock;

    @Before
    public void setUp() throws Exception {

        when(redisConnectionConfigMock.getDatabase()).thenReturn(0);
        when(redisConnectionConfigMock.getKey()).thenReturn("dummyKey");
        when(redisConnectionConfigMock.getPassword()).thenReturn("secret");

        doReturn(jedisPoolMock).when(jedisPoolFactory).createJedisPool();
        doReturn(jedisSentinelPoolMock).when(jedisPoolFactory).createJedisSentinelPool();
    }

    @Test
    public void ifSchemeIsNode_aJedisPoolIsCreated() throws Exception {

        when(redisConnectionConfigMock.getScheme()).thenReturn(RedisConnectionConfig.RedisScheme.NODE);

        final Pool<Jedis> pool = jedisPoolFactory.createPool();

        assertThat(pool, is(instanceOf(JedisPool.class)));
    }

    @Test
    public void ifSchemeIsSentinel_aJedisSentinelPoolIsCreated() throws Exception {

        when(redisConnectionConfigMock.getScheme()).thenReturn(RedisConnectionConfig.RedisScheme.SENTINEL);

        final Pool<Jedis> pool = jedisPoolFactory.createPool();

        assertThat(pool, is(instanceOf(JedisSentinelPool.class)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void anExceptionIsThrownDuringJedisPoolCreationWithNullScheme() throws Exception {

        when(redisConnectionConfigMock.getScheme()).thenReturn(null);

        final Pool<Jedis> pool = jedisPoolFactory.createPool();

        assertThat(pool, is(instanceOf(JedisPool.class)));
    }

    @Test
    public void sentinelsCanBeExtracted() throws Exception {

        final Set<String> sentinels = jedisPoolFactory.getSentinels("om-test-02:6379, om-test-03:6379");

        assertThat(sentinels, containsInAnyOrder("om-test-02:6379", "om-test-03:6379"));
    }
}
