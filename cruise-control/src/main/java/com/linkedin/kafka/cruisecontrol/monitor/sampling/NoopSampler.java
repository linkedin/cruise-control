/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;


public class NoopSampler implements MetricSampler {

  @Override
  public Samples getSamples(Cluster cluster, Set<TopicPartition> assignedPartitions, long startTimeMs, long endTimeMs,
                            SamplingMode mode, MetricDef metricDef, long timeout) throws MetricSamplingException {
    return null;
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public void close() throws Exception {

  }
}
