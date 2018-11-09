package de.idealo.logback.appender.jediswriter;

import de.idealo.logback.appender.jedisclient.RedisConnectionConfig;

import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.DeferredProcessingAware;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor
@Getter
@Builder
public class JedisWriterConfiguration {
    private final int maxInitializeTries;
    private final long retryInitializeIntervalMillis;
    private final int maxBufferedMessages;
    private final long flushBufferIntervalMillis;
    @NonNull
    private final Encoder<DeferredProcessingAware> encoder;
    @NonNull
    private final RedisConnectionConfig connectionConfig;
}
