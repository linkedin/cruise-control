/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * A KafkaCruiseControl extension with asynchronous operations support.
 *
 * The following async requests are supported: {@link com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable}.
 *
 * The other operations are non-blocking by default.
 */
public class AsyncKafkaCruiseControl extends KafkaCruiseControl {
  // TODO: Make this configurable.
  private static final int NUM_SESSION_EXECUTOR_THREADS = 3;
  private final ExecutorService _sessionExecutor;

  /**
   * Construct the Cruise Control
   *
   * @param config the configuration of Cruise Control.
   * @param dropwizardMetricRegistry The metric registry that holds all the metrics for monitoring Cruise Control.
   */
  public AsyncKafkaCruiseControl(KafkaCruiseControlConfig config, MetricRegistry dropwizardMetricRegistry) {
    super(config, dropwizardMetricRegistry);
    _sessionExecutor = Executors.newFixedThreadPool(NUM_SESSION_EXECUTOR_THREADS,
                                                    new KafkaCruiseControlThreadFactory("ServletSessionExecutor"));
  }

  /**
   * @return Session executor.
   */
  public ExecutorService sessionExecutor() {
    return _sessionExecutor;
  }
}
