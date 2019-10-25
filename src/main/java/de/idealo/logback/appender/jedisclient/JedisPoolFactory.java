package de.idealo.logback.appender.jedisclient;

import java.util.Arrays;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

/**
 * Factory for Jedis pool creation. Supports the creation of clients for single and sentinel Redis installations.
 *
 * @see <a href="http://redis.io/topics/sentinel">redis sentinel</a>
 * @see <a href="https://github.com/xetorthio/jedis/issues/725">jedis client for redis sentinel</a>
 */
public class JedisPoolFactory {

    private final JedisPoolCreator poolCreator;

    public JedisPoolFactory(JedisPoolCreator poolCreator) {
        this.poolCreator = poolCreator;
    }

    public Pool<Jedis> createPool(RedisConnectionConfig connectionConfig) {
        final RedisConnectionConfig.RedisScheme scheme = connectionConfig.getScheme();
        if (scheme == null) {
            throw getUnsupportedSchemeException();
        }
        switch (scheme) {
            case NODE:
                return poolCreator.createJedisPool(connectionConfig);
            case SENTINEL:
                return poolCreator.createJedisSentinelPool(connectionConfig);
            default:
                throw getUnsupportedSchemeException();
        }
    }

    private IllegalArgumentException getUnsupportedSchemeException() {
        throw new IllegalArgumentException("Scheme is not supported, only "
                + Arrays.asList(RedisConnectionConfig.RedisScheme.values()));
    }
}
