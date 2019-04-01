package com.linkedin.kafka.cruisecontrol.detector.notifier;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SlackMessageTest {

    @Test
    public void testSlackMessageJsonFormat() {
        String expectedJson = "{\"username\" : \"userA\",\"text\" : \"cc alert\",\"icon_emoji\" : \":information_source:\",\"channel\" : \"#cc-alerts\"}";
        assertEquals(expectedJson, new SlackMessage("userA", "cc alert", ":information_source:", "#cc-alerts").toString());
    }
}
