/*
 *
 *  * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *  
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;


/**
 * The {@link Entity} class used by {@link MetricSampleAggregator} for brokers.
 */
public class BrokerEntity extends Entity<String> {
  private final String _rack;
  private final int _brokerId;
  
  public BrokerEntity(String rack, int brokerId) {
    _rack = rack;
    _brokerId = brokerId;
  }
  
  @Override
  public String group() {
    return _rack;
  }

  @Override
  public int hashCode() {
    return _brokerId;
  }

  @Override
  public boolean equals(Object other) {
    return other != null && other instanceof BrokerEntity && ((BrokerEntity) other)._brokerId == _brokerId;
  }
}
