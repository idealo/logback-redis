package de.idealo.logback.appender;

import java.io.Closeable;
import java.util.Optional;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class JedisClientProvider implements Closeable {
    private final RedisConnectionConfig connectionConfig;
    private final JedisPoolFactory poolFactory;

    private Pool<Jedis> pool;

    public JedisClientProvider(JedisPoolFactory poolFactory, RedisConnectionConfig connectionConfig) {
        this.poolFactory = poolFactory;
        this.connectionConfig = connectionConfig;
    }

    public Optional<Jedis> getJedisClient() {
        try {
            return Optional.of(getPool().getResource());
        } catch (Exception ex) {
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
