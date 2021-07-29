/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.vertx;

import com.linkedin.kafka.cruisecontrol.CruiseControlVertxIntegrationTestHarness;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

public class VertxResultIntegrationTest extends CruiseControlVertxIntegrationTestHarness {
    @Before
    public void start() throws Exception {
        super.start();
    }

    @After
    public void teardown() {
        super.stop();
    }

    public void testKafkaClusterStateResponse() throws Exception {
        assertEquals(getVertxResult("kafka_cluster_state", _vertxPort), getServletResult("kafka_cluster_state", _servletPort));
        assertEquals(getServletResult("kafka_cluster_state?json=true", _servletPort), getVertxResult("kafka_cluster_state?json=true", _vertxPort));
        assertEquals(getServletResult("kafka_cluster_state?verbose=true", _servletPort),
            getVertxResult("kafka_cluster_state?verbose=true", _vertxPort));
        assertEquals(getServletResult("kafka_cluster_state?verbose=true&json=true", _servletPort),
            getVertxResult("kafka_cluster_state?verbose=true&json=true", _vertxPort));
    }

    public void testCruiseControlStateResponse() throws Exception {
        assertEquals(getVertxResult("state", _vertxPort), getServletResult("state", _servletPort));
        assertEquals(getServletResult("state?json=true", _servletPort), getVertxResult("state?json=true", _vertxPort));
        assertEquals(getServletResult("state?verbose=true", _servletPort),
                getVertxResult("state?verbose=true", _vertxPort));
        assertEquals(getServletResult("state?verbose=true&json=true", _servletPort),
                getVertxResult("state?verbose=true&json=true", _vertxPort));
        assertEquals(getServletResult("state?substates=analyzer,executor", _servletPort),
                getVertxResult("state?substates=analyzer,executor", _vertxPort));
    }

    public void testLoadResponse() throws Exception {
        assertEquals(getVertxResult("load", _vertxPort), getServletResult("load", _servletPort));
        assertEquals(getServletResult("load?json=true", _servletPort), getVertxResult("load?json=true", _vertxPort));
        assertEquals(getServletResult("load?verbose=true", _servletPort),
                getVertxResult("load?verbose=true", _vertxPort));
        assertEquals(getServletResult("load?verbose=true&json=true", _servletPort),
                getVertxResult("load?verbose=true&json=true", _vertxPort));
    }

    public void testPartitionLoadResponse() throws Exception {
        assertEquals(getVertxResult("partition_load", _vertxPort), getServletResult("partition_load", _servletPort));
        assertEquals(getServletResult("partition_load?json=true", _servletPort), getVertxResult("partition_load?json=true", _vertxPort));
        assertEquals(getServletResult("partition_load?verbose=true", _servletPort),
                getVertxResult("partition_load?verbose=true", _vertxPort));
        assertEquals(getServletResult("partition_load?verbose=true&json=true", _servletPort),
                getVertxResult("partition_load?verbose=true&json=true", _vertxPort));
    }
}
