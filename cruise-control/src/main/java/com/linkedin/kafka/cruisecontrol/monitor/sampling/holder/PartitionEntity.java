/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.cruisecontrol.model.Entity;
import org.apache.kafka.common.TopicPartition;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;


/**
 * The {@link Entity} class used by {@link MetricSampleAggregator}
 */
public class PartitionEntity extends Entity<String> {
  private final TopicPartition _tp;
  public PartitionEntity(TopicPartition tp) {
    _tp = tp;
  }

  @Override
  public String group() {
    return _tp.topic();
  }

  public TopicPartition tp() {
    return _tp;
  }

  @Override
  public int hashCode() {
    return _tp.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof PartitionEntity && _tp.equals(((PartitionEntity) other).tp());
  }
}
