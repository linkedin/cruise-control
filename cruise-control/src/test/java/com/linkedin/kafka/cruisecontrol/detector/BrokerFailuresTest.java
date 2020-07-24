/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class BrokerFailuresTest {
    @Test
    public void testToString() {
        assertNotNull(new BrokerFailures().toString());
    }
}
