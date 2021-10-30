/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class MSTeamsMessageTest {

    @Test
    public void testMSTeamsMessageJsonFormat() {
        String expectedJson = "{\"@type\": \"MessageCard\",\"@context\": \"http://schema.org/extensions\","
            + "\"themeColor\": \"0076D7\",\"summary\": \"Cruise-Control Alert\",\"sections\": "
            + "[{\"facts\": [{\"name\": \"title1\", \"value\": \"description1\"}]}]}";
        assertEquals(expectedJson, new MSTeamsMessage(Map.of("title1", "description1")).toString());
    }
}
