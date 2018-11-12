package de.idealo.logback.appender.jediswriter;

import java.util.function.Function;

import de.idealo.logback.appender.jedisclient.JedisClient;

import ch.qos.logback.core.spi.DeferredProcessingAware;
import redis.clients.jedis.Pipeline;

public class BufferedJedisPublisher extends AbstractBufferedJedisWriter {

    BufferedJedisPublisher(JedisClient client,
            Function<DeferredProcessingAware, String> messageCreator,
            String redisKey,
            int maxBufferItems,
            long flushBufferIntervalMillis) {
        super(client, messageCreator, redisKey, maxBufferItems, flushBufferIntervalMillis);
    }

    @Override
    void addValuesToPipeline(Pipeline pipeline, String... values) {
        for (String value : values) {
            pipeline.publish(getRedisKey(), value);
        }
    }
}
