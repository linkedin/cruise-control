/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.vertx.MainVerticle;
import io.vertx.core.Vertx;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This is the main entry point for the Vertx based API.
 */
public class KafkaCruiseControlVertxApp extends KafkaCruiseControlApp {

    protected static MainVerticle verticle;
    private static Vertx vertx;

    KafkaCruiseControlVertxApp(KafkaCruiseControlConfig config, Integer port, String hostname) {
        super(config, port, hostname);
        vertx = Vertx.vertx();
    }

    //visible for testing
    KafkaCruiseControlVertxApp(KafkaCruiseControlConfig config, Integer port,
                               String hostname, AsyncKafkaCruiseControl asyncKafkaCruiseControl, MetricRegistry metricRegistry) {
        super(config, port, hostname, asyncKafkaCruiseControl, metricRegistry);
        vertx = Vertx.vertx();
        verticle = new MainVerticle(_kafkaCruiseControl, _metricRegistry, _port, _hostname);
    }

    @Override
    public String serverUrl() {
        return null;
    }

    @Override
    void start() {
        CountDownLatch startupLatch = new CountDownLatch(1);
        if (LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.NOT_STARTED
                .equals(_kafkaCruiseControl.getLoadMonitorTaskRunnerState())) {
            _kafkaCruiseControl.startUp();
        }

        verticle = new MainVerticle(_kafkaCruiseControl, _metricRegistry, _port, _hostname);
        vertx.deployVerticle(verticle, event -> {
            if (event.failed()) {
                throw new RuntimeException("Startup failed", event.cause());
            }
            startupLatch.countDown();
        });
        try {
            startupLatch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Startup interrupted", e);
        }
    }

    @Override
    void stop() {
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        vertx.close(event -> {
            super.stop();
            if (event.failed()) {
                throw new RuntimeException("Sutdown failed", event.cause());
            }
            shutdownLatch.countDown();
        });
        try {
            shutdownLatch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Startup interrupted", e);
        }
    }

    //visible for testing
    static MainVerticle getVerticle() throws Exception {
        if (verticle == null) {
            throw new Exception();
        }
        return verticle;
    }
}
