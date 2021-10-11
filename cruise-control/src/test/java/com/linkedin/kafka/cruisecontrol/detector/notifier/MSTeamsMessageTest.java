/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class MSTeamsMessageTest {

    @Test
    public void testSlackMessageJsonFormat() {
        String expectedJson = "{\"@type\": \"MessageCard\",\"@context\": \"http://schema.org/extensions\","
            + "\"themeColor\": \"0076D7\",\"summary\": \"Cruise-Control Alert\",\"sections\": "
            + "[{\"facts\": [{\"name\": \"title1\", \"value\": \"description1\"},"
            + "{\"name\": \"title2\", \"value\": \"description2\"}]}]}";
        assertEquals(expectedJson,
            new MSTeamsMessage(Map.of("title1", "description1", "title2", "description2")).toString());
    }
}
