package de.idealo.logback.appender.jediswriter;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import de.idealo.logback.appender.jedisclient.JedisPoolFactory;
import de.idealo.logback.appender.jedisclient.RedisConnectionConfig;
import de.idealo.logback.appender.jedisclient.RedisConnectionConfig.Method;

public class BufferedJedisWriterFactoryTest {
    @Mock
    private JedisPoolFactory jedisPoolFactory;
    @Mock
    private JedisWriterConfiguration writerConfiguration;
    @Mock
    private RedisConnectionConfig connectionConfig;
    @InjectMocks
    private BufferedJedisWriterFactory bufferedJedisWriterFactory;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(writerConfiguration.getConnectionConfig()).thenReturn(connectionConfig);
        when(writerConfiguration.getMaxInitializeTries()).thenReturn(1);
    }

    @Test
    public void create_publish_writer_on_publish_writer_type() throws Exception {
        when(connectionConfig.getMethod()).thenReturn(Method.PUBLISH);
        assertThat(bufferedJedisWriterFactory.createJedisWriter(writerConfiguration), is(instanceOf(BufferedJedisPublisher.class)));
    }

    @Test
    public void create_rpush_writer_pool_on_rpush_writer_type() throws Exception {
        when(connectionConfig.getMethod()).thenReturn(Method.RPUSH);
        assertThat(bufferedJedisWriterFactory.createJedisWriter(writerConfiguration), is(instanceOf(BufferedJedisRPusher.class)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void exception_on_null_writer_type() throws Exception {
        when(connectionConfig.getMethod()).thenReturn(null);
        bufferedJedisWriterFactory.createJedisWriter(writerConfiguration);
    }

}
