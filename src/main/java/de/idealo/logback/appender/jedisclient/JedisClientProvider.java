package de.idealo.logback.appender.jedisclient;

import java.io.Closeable;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class JedisClientProvider implements Closeable {

    /*
     * logger must not be static: logback may not be initialized, when class is loaded.
     * creating an final static field at this time may result in a null reference
     */
    private final Logger log;

    private final RedisConnectionConfig connectionConfig;
    private final JedisPoolFactory poolFactory;

    private Pool<Jedis> pool;

    public JedisClientProvider(JedisPoolFactory poolFactory, RedisConnectionConfig connectionConfig) {
        log = LoggerFactory.getLogger(getClass());
        this.poolFactory = poolFactory;
        this.connectionConfig = connectionConfig;
    }

    public Optional<Jedis> getJedisClient() {
        try {
            return Optional.of(getPool().getResource());
        } catch (Exception ex) {
            log.warn("unable to provide jedis client", ex);
            return Optional.empty();
        }
    }

    private synchronized Pool<Jedis> getPool() {
        if (pool == null) {
            pool = poolFactory.createPool(connectionConfig);
        }
        return pool;
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }
}
