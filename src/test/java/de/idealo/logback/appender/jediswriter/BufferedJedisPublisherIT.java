package de.idealo.logback.appender.jediswriter;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.idealo.logback.appender.jedisclient.JedisClient;
import de.idealo.logback.appender.jedisclient.JedisClientProvider;

import ch.qos.logback.core.spi.DeferredProcessingAware;
import lombok.AllArgsConstructor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

public class BufferedJedisPublisherIT {
    private static final long BUFFER_FLUSH_MILLIS = 100L;
    private static final String CHANNEL = "testChannel";
    private static final Function<DeferredProcessingAware, String> MESSAGE_CREATOR = String::valueOf;
    private static final Set<DeferredProcessingAware> EVENTS = IntStream.range(0, 10)
            .mapToObj(i -> mock(DeferredProcessingAware.class))
            .collect(toSet());

    private RedisServer redisServer;
    private Jedis redisSender;
    private Jedis redisSubscriber;

    @Before
    public void init() throws Exception {
        final int port = getRandomPort();
        redisServer = new RedisServerBuilder()
                .port(port)
                .build();
        redisServer.start();
        redisSender = new JedisPool("localhost", port).getResource();
        redisSubscriber = new JedisPool("localhost", port).getResource();
    }

    private static int getRandomPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    @After
    public void shutdown() throws Exception {
        redisSender.close();
        redisSubscriber.close();
        redisServer.stop();
    }

    @Test
    public void published_messages_are_sent() throws InterruptedException {
        JedisClientProvider clientProvider = mock(JedisClientProvider.class);
        when(clientProvider.getJedisClient()).thenReturn(Optional.of(redisSender));
        final JedisClient jedisClient = new JedisClient(clientProvider, 1, 0L);

        try (BufferedJedisPublisher publisher = new BufferedJedisPublisher(jedisClient, MESSAGE_CREATOR, CHANNEL, 1, BUFFER_FLUSH_MILLIS)) {
            final CountDownLatch receiverStarted = new CountDownLatch(1);
            final CountDownLatch messagesReceived = new CountDownLatch(EVENTS.size());
            final ValueReceiver valueReceiver = new ValueReceiver(redisSubscriber, receiverStarted, messagesReceived);

            final Thread receiverThread = new Thread(valueReceiver);
            receiverThread.start();
            receiverStarted.await();

            final Set<String> sentMessages = new HashSet<>(EVENTS.size());
            for (DeferredProcessingAware event : EVENTS) {
                publisher.append(event);
                sentMessages.add(MESSAGE_CREATOR.apply(event));
            }
            messagesReceived.await(5 * BUFFER_FLUSH_MILLIS, TimeUnit.MILLISECONDS);
            valueReceiver.unsubscribe();

            assertThat(valueReceiver.receivedMessages, is(equalTo(sentMessages)));
        }
    }

    @AllArgsConstructor
    private static final class ValueReceiver extends JedisPubSub implements Runnable {
        private final Jedis redisSubscriber;
        private final CountDownLatch receiverStarted;
        private final CountDownLatch messageReceivedLatch;
        final Set<String> receivedMessages = new HashSet<>();

        @Override
        public void onMessage(String channel, String message) {
            receivedMessages.add(message);
            messageReceivedLatch.countDown();
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            receiverStarted.countDown();
        }

        @Override
        public void run() {
            redisSubscriber.subscribe(this, CHANNEL);
        }
    }
}
