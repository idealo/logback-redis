package de.idealo.logback.appender.jedisclient;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

/**
 * Factory for Jedis pool creation. Supports the creation of clients for single and sentinel Redis installations.
 *
 * @see <a href="http://redis.io/topics/sentinel">redis sentinel</a>
 * @see <a href="https://github.com/xetorthio/jedis/issues/725">jedis client for redis sentinel</a>
 */
public class JedisPoolCreator {

    private final GenericObjectPoolConfig objectPoolConfig;

    public JedisPoolCreator() {
        objectPoolConfig = new GenericObjectPoolConfig();
        objectPoolConfig.setTestOnBorrow(true);
    }

    public JedisPool createJedisPool(RedisConnectionConfig connectionConfig) {
        return new JedisPool(objectPoolConfig, connectionConfig.getHost(), connectionConfig.getPort(),
                connectionConfig.getTimeout(), connectionConfig.getPassword(), connectionConfig.getDatabase(), connectionConfig.isSsl());
    }

    public JedisSentinelPool createJedisSentinelPool(RedisConnectionConfig connectionConfig) {
        return new JedisSentinelPool(connectionConfig.getSentinelMasterName(),
                getSentinels(connectionConfig.getSentinels()),
                objectPoolConfig, connectionConfig.getTimeout(), connectionConfig.getPassword(), connectionConfig.getDatabase());
    }

    static Set<String> getSentinels(String sentinelsAsString) {
        return Arrays.stream(sentinelsAsString.split(",")).map(String::trim).collect(Collectors.toSet());
    }
}
