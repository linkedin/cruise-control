/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.buildTopicRegex;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit test class for anomaly utilization methods.
 */
public class AnomalyUtilsTest {
    private static final String TOPIC1 = "abc";
    private static final String TOPIC2 = "def";
    private static final String TOPIC3 = "ghi";

    @Test
    public void testBuildTopicRegex() {
        Set<String> topicsToMatch = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(TOPIC1, TOPIC2)));
        Pattern pattern = buildTopicRegex(topicsToMatch);

        assertTrue(pattern.matcher(TOPIC1).matches());
        assertTrue(pattern.matcher(TOPIC2).matches());
        assertFalse(pattern.matcher(TOPIC3).matches());
    }
}
