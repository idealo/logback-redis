package de.idealo.logback.appender;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

/**
 * Factory for Jedis pool creation. Supports the creation of clients for single and sentinel Redis installations.
 *
 * @see <a href="http://redis.io/topics/sentinel">redis sentinel</a>
 * @see <a href="https://github.com/xetorthio/jedis/issues/725">jedis client for redis sentinel</a>
 */
public class JedisPoolFactory {

    private final GenericObjectPoolConfig objectPoolConfig;

    public JedisPoolFactory() {
        objectPoolConfig = new GenericObjectPoolConfig();
        objectPoolConfig.setTestOnBorrow(true);
    }

    public Pool<Jedis> createPool(RedisConnectionConfig connectionConfig) {
        final RedisConnectionConfig.RedisScheme scheme = connectionConfig.getScheme();
        if (scheme == null) {
            throw createExceptionForNotSupportedScheme();
        }

        switch (scheme) {
            case NODE:
                return createJedisPool(connectionConfig);
            case SENTINEL:
                return createJedisSentinelPool(connectionConfig);
            default:
                throw createExceptionForNotSupportedScheme();
        }
    }

    private IllegalArgumentException createExceptionForNotSupportedScheme() {
        throw new IllegalArgumentException("Scheme is not supported, only "
                + Arrays.asList(RedisConnectionConfig.RedisScheme.values()));
    }

    Pool<Jedis> createJedisPool(RedisConnectionConfig connectionConfig) {
        return new JedisPool(objectPoolConfig, connectionConfig.getHost(), connectionConfig.getPort(),
                connectionConfig.getTimeout(), connectionConfig.getPassword(), connectionConfig.getDatabase(), connectionConfig.isSsl());
    }

    Pool<Jedis> createJedisSentinelPool(RedisConnectionConfig connectionConfig) {
        return new JedisSentinelPool(connectionConfig.getSentinelMasterName(),
                getSentinels(connectionConfig.getSentinels()),
                objectPoolConfig, connectionConfig.getTimeout(), connectionConfig.getPassword(), connectionConfig.getDatabase());
    }

    Set<String> getSentinels(String sentinelsAsString) {
        return Arrays.stream(sentinelsAsString.split(",")).map(String::trim).collect(Collectors.toSet());
    }
}
