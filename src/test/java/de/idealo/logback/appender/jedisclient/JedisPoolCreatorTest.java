package de.idealo.logback.appender.jedisclient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Set;

import org.junit.Test;

public class JedisPoolCreatorTest {

    @Test
    public void sentinels_can_be_extracted() throws Exception {

        final Set<String> sentinels = JedisPoolCreator.getSentinels("om-test-02:6379, om-test-03:6379");

        assertThat(sentinels, containsInAnyOrder("om-test-02:6379", "om-test-03:6379"));
    }
}
