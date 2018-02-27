package de.idealo.logback.appender;

import redis.clients.jedis.Protocol;

/**
 * Configuration of all redis connection related properties.
 */
public class RedisConnectionConfig {
    private RedisScheme scheme;
    private String host = Protocol.DEFAULT_HOST;
    private int port = Protocol.DEFAULT_PORT;
    private String key = null;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private String password = null;
    private int database = Protocol.DEFAULT_DATABASE;
    private boolean ssl = false;
    private String sentinels;
    private String sentinelMasterName;

    public RedisScheme getScheme() {
        return scheme;
    }

    public void setScheme(RedisScheme scheme) {
        this.scheme = scheme;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getSentinels() {
        return sentinels;
    }

    public void setSentinels(String sentinels) {
        this.sentinels = sentinels;
    }

    public String getSentinelMasterName() {
        return sentinelMasterName;
    }

    public void setSentinelMasterName(String sentinelMasterName) {
        this.sentinelMasterName = sentinelMasterName;
    }

    public boolean isSsl() { return ssl; }

    public void setSsl(boolean ssl) { this.ssl = ssl; }

    public enum RedisScheme {
        NODE, SENTINEL;
    }
}
