package de.idealo.logback.appender;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.slf4j.LoggerFactory;

import de.idealo.logback.appender.jedisclient.RedisConnectionConfig;
import de.idealo.logback.appender.utils.MDCUtils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.pattern.PatternLayoutEncoderBase;
import ch.qos.logback.core.util.StatusPrinter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.embedded.RedisCluster;
import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;
import redis.embedded.util.JedisUtil;

/**
 * Integration test that uses an <a href="https://github.com/kstyrc/embedded-redis">embedded redis</a>.
 */
public class RedisBatchAppenderEmbeddedIT {

    private static final Map<String, String> DEFAULT_REDIS_LOG_MESSAGE = unmodifiableMap(createDefaultRedisLogMessage());
    private static final int LOCAL_REDIS_PORT = 6379;
    private static final int REDIS_IDLE_TIMEOUT_IN_SECONDS = 3;
    private static final int SLEEP_TIME_IN_SECONDS_FOR_CONNECTION_TIMEOUT = REDIS_IDLE_TIMEOUT_IN_SECONDS + 2;
    private static final String REDIS_KEY_INTEGRATIONTEST = "integrationtest";
    private static final MDCUtils MDC_UTILS = new MDCUtils();

    /** defined as retryInitializeIntervalInSeconds in logback-xml.xml */
    private static final long RETRY_INITIALIZE_INTERVAL_IN_SECONDS = 1L;
    /** defined logback-xml.xml */
    private static final String REDIS_LOGGER_NAME = "LoggingTest";

    private RedisServer redisServer;
    private Jedis redisClient;

    private final AtomicLong lastSequenceId = new AtomicLong(0);

    @Before
    public void init() throws Exception {
        redisServer = new RedisServerBuilder()
                .port(LOCAL_REDIS_PORT)
                .setting("timeout " + REDIS_IDLE_TIMEOUT_IN_SECONDS)
                .build();
        redisServer.start(); // this waits until server write start notification to stdout (!)
        log().info("started redis server");

        TimeUnit.SECONDS.sleep(RETRY_INITIALIZE_INTERVAL_IN_SECONDS); // makes sure jedis client is initialized

        redisClient = new JedisPool("localhost", LOCAL_REDIS_PORT).getResource(); // uses localhost / local port
        log().info("initialized redis client");
        logStatus();
    }

    @After
    public void shutdown() throws Exception {
        if (redisClient != null) {
            redisClient.close();
        }
        log().info("closed redis client");

        if (redisServer != null) {
            redisServer.stop(); // does not seem to be called if assertion fails...
        }
        log().info("stopped redis server");
    }

    @Test
    public void all_messages_are_successfully_logged_before_and_after_connection_timeout() throws Exception {
        messageIsSuccessfullyLogged();
        log().info("all messages are successfully logged before connection timeout");

        log().info("waiting {} seconds before logging to redis again", SLEEP_TIME_IN_SECONDS_FOR_CONNECTION_TIMEOUT);
        TimeUnit.SECONDS.sleep(SLEEP_TIME_IN_SECONDS_FOR_CONNECTION_TIMEOUT);

        // get fresh connection from the pool; previous one has meanwhile timed out
        redisClient = new JedisPool("localhost", LOCAL_REDIS_PORT).getResource();

        messageIsSuccessfullyLogged();
        log().info("all messages are successfully logged after connection timeout");
    }

    @Test
    public void all_messages_are_successfully_logged_after_redis_was_temporarily_not_available() throws Exception {
        redisServer.stop();
        log().info("stopped redis server");
        logMessages(1, 1, "message during stopped redis");

        redisServer.start();
        log().info("re-started redis server");
        // get fresh connection from the pool
        redisClient = new JedisPool("localhost", LOCAL_REDIS_PORT).getResource();

        assertNoMessagesInRedis();

        messageIsSuccessfullyLogged();
        messageIsSuccessfullyLogged();
        log().info("all messages are successfully logged after redis server was temporarily not available");
    }

    @Test
    public void dont_fail_on_unreachable_redis_server() {
        redisServer.stop();

        RedisBatchAppender redisBatchAppender = new RedisBatchAppender();
        redisBatchAppender.setConnectionConfig(getRedisConnectionConfig());
        redisBatchAppender.setEncoder(new PatternLayoutEncoderBase<>());

        // action
        redisBatchAppender.start();
        redisServer.start();
    }

    @Test
    public void retry_connection_after_interval() throws InterruptedException {
        RedisCluster cluster = RedisCluster.builder().ephemeral().sentinelCount(3).quorumSize(2)
                .replicationGroup("master1", 1)
                .replicationGroup("master2", 1)
                .replicationGroup("master3", 1)
                .build();
        cluster.start();
        Set<String> jedisSentinelHosts = JedisUtil.sentinelHosts(cluster);

        // simulate cluster outage
        cluster.stop();

        try {
            RedisConnectionConfig connectionConfig = new RedisConnectionConfig();
            connectionConfig.setSentinels(jedisSentinelHosts.stream().collect(joining(",")));
            connectionConfig.setScheme(RedisConnectionConfig.RedisScheme.SENTINEL);
            connectionConfig.setSentinelMasterName("master1");

            RedisBatchAppender redisBatchAppender = spy(new RedisBatchAppender());
            redisBatchAppender.setConnectionConfig(connectionConfig);
            redisBatchAppender.setEncoder(new PatternLayoutEncoderBase<>());
            redisBatchAppender.setRetryInitializeIntervalInSeconds(1);

            ((Logger) LoggerFactory.getLogger(RedisBatchAppender.class)).setLevel(Level.OFF);

            // action, should fail
            redisBatchAppender.start();
            // restart cluster
            cluster.start();

            TimeUnit.SECONDS.sleep(2);

            //assertThat("should succeed one time", redisBatchAppender.getConnectionStartupCounter() == 1);
            redisBatchAppender.stop();
        } finally {
            if (cluster.isActive()) {
                cluster.stop();
            }
        }
    }

    private static RedisConnectionConfig getRedisConnectionConfig() {
        RedisConnectionConfig connectionConfig = new RedisConnectionConfig();
        connectionConfig.setSentinels("localhost," + "127.0.0.1:" + LOCAL_REDIS_PORT);
        connectionConfig.setScheme(RedisConnectionConfig.RedisScheme.SENTINEL);
        connectionConfig.setKey("x");
        connectionConfig.setSentinelMasterName("mymaster");
        return connectionConfig;
    }

    private void assertNoMessagesInRedis() {
        String loggedMessage = redisClient.lindex(REDIS_KEY_INTEGRATIONTEST, 0);
        assertThat(loggedMessage, is(nullValue()));
    }

    private void messageIsSuccessfullyLogged() throws Exception {
        redisClient.del(REDIS_KEY_INTEGRATIONTEST);
        logMessages(1, 1, "dummy");

        final JSONObject expectedLoggedMessage = getExpectedObject(lastSequenceId.get(), "dummy", 0L);
        log().debug("expected logged message: {}", expectedLoggedMessage);

        String loggedMessage = redisClient.lindex(REDIS_KEY_INTEGRATIONTEST, 0);

        log().debug("logged message: {}", loggedMessage);

        assertThat(loggedMessage, is(not(nullValue())));
        JSONAssert.assertEquals(expectedLoggedMessage + " and " + loggedMessage + " should be equal", loggedMessage, expectedLoggedMessage, false);
    }

    private JSONObject getExpectedObject(long expectedSequenceNumber, String messagePrefix, long messageId) {
        try {
            final JSONObject result = new JSONObject(DEFAULT_REDIS_LOG_MESSAGE);
            result.put("seq", Long.toString(expectedSequenceNumber));
            result.put("message", messagePrefix + ":" + messageId);
            result.put("thread", Thread.currentThread().getName());
            return result;
        } catch (JSONException ex) {
            return null;
        }
    }

    private void logMessages(long count, int statsInterval, String prefix) {
        Logger loggingTest = createRedisLogger();

        for (int i = 0; i < count; i++) {
            MDC_UTILS.clear()
                    .putIfPresent("seq", lastSequenceId.incrementAndGet())
                    .putIfPresent("someNumber", 1L)
                    .putIfPresent("someString", "hello");
            loggingTest.info(prefix + ":" + i);

            int messagesSent = i + 1;
            if (messagesSent % statsInterval == 0) {
                log().info("Messages sent so far: {}", messagesSent);
            }
        }
        log().info("Messages sent TOTAL: {}", count);
    }

    // lazy initialization to defer loading of logback XML until embedded redis is started
    private static org.slf4j.Logger log() {
        return LoggerFactory.getLogger(RedisBatchAppenderEmbeddedIT.class);
    }

    private static void logStatus() {
        // assume SLF4J is bound to logback in the current environment
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        // print logback's internal status
        StatusPrinter.print(lc);
    }

    private static Logger createRedisLogger() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        return lc.getLogger(REDIS_LOGGER_NAME);
    }

    private static Map<String, String> createDefaultRedisLogMessage() {
        final Map<String, String> expectedMessage = new HashMap<>();
        expectedMessage.put("someNumber", "1");
        expectedMessage.put("someString", "hello");
        expectedMessage.put("logger", REDIS_LOGGER_NAME);
        expectedMessage.put("level", "INFO");
        expectedMessage.put("file", RedisBatchAppenderEmbeddedIT.class.getSimpleName() + ".java");
        return expectedMessage;
    }
}
