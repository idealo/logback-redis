package de.idealo.logback.appender.jedisclient;

import lombok.Getter;
import lombok.Setter;
import redis.clients.jedis.Protocol;

/**
 * Configuration of all redis connection related properties.
 */
@Getter
@Setter
public class RedisConnectionConfig {
    private RedisScheme scheme;
    private Method method = Method.RPUSH;
    private String host = Protocol.DEFAULT_HOST;
    private int port = Protocol.DEFAULT_PORT;
    private String key = null;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private String password = null;
    private int database = Protocol.DEFAULT_DATABASE;
    private boolean ssl = false;
    private String sentinels;
    private String sentinelMasterName;

    public enum RedisScheme {
        NODE,
        SENTINEL;
    }

    public enum Method {
        RPUSH,
        PUBLISH;
    }
}
