package de.idealo.logback.appender;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private final RedisConnectionConfig connectionConfig;
    private final GenericObjectPoolConfig objectPoolConfig;

    public JedisPoolFactory(RedisConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;

        this.objectPoolConfig = new GenericObjectPoolConfig();
        objectPoolConfig.setTestOnBorrow(true);
    }

    public Pool<Jedis> createPool() {
        final RedisConnectionConfig.RedisScheme scheme = connectionConfig.getScheme();
        if (scheme == null) {
            throw createExceptionForNotSupportedScheme();
        }

        switch (scheme) {
            case NODE: {
                return createJedisPool();
            }
            case SENTINEL: {
                return createJedisSentinelPool();
            }
            default: {
                throw createExceptionForNotSupportedScheme();
            }
        }
    }

    private IllegalArgumentException createExceptionForNotSupportedScheme() {
        throw new IllegalArgumentException("Scheme is not supported, only "
                + Arrays.asList(RedisConnectionConfig.RedisScheme.values()));
    }

    Pool<Jedis> createJedisPool() {
        return new JedisPool(objectPoolConfig, connectionConfig.getHost(), connectionConfig.getPort(),
                connectionConfig.getTimeout(), connectionConfig.getPassword(), connectionConfig.getDatabase());
    }

    Pool<Jedis> createJedisSentinelPool() {
        return new JedisSentinelPool(connectionConfig.getSentinelMasterName(),
                getSentinels(connectionConfig.getSentinels()),
                objectPoolConfig, connectionConfig.getTimeout(), connectionConfig.getPassword(), connectionConfig.getDatabase());
    }

    Set<String> getSentinels(String sentinelsAsString) {
        return Stream.of(sentinelsAsString.split(",")).map(sentinel -> sentinel.trim()).collect(Collectors.toSet());
    }
}
