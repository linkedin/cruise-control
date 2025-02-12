/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

public class CCAbstractKRaftTestHarness {
    protected CCEmbeddedKRaftController _controller;

    /**
     * Setup the unit test.
     */
    public void setUp() {
        if (_controller == null) {
            _controller = new CCEmbeddedKRaftController();
        }
        _controller.startup();
    }

    /**
     * Teardown the unit test.
     */
    public void tearDown() {
        if (_controller != null) {
            CCKafkaTestUtils.quietly(() -> _controller.close());
            _controller = null;
        }
    }

    protected CCEmbeddedKRaftController kraftController() {
        return _controller;
    }
}
