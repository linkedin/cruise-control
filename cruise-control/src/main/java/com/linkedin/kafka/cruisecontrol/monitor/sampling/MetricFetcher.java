/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that is responsible for fetching the metrics from the cluster.
 * The metrics may be used for either load model training purpose or just for cluster workload monitoring.
 */
abstract class MetricFetcher implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricFetcher.class);

  @Override
  public Boolean call() {
    boolean hasSamplingError = false;

    try {
      fetchMetricsForAssignedPartitions();
    } catch (MetricSamplingException mse) {
      LOG.warn("Received sampling error.");
      hasSamplingError = true;
    } catch (Throwable t) {
      LOG.error("Received exception.", t);
      hasSamplingError = true;
    }

    return hasSamplingError;
  }

  protected abstract void fetchMetricsForAssignedPartitions() throws MetricSamplingException;

}
