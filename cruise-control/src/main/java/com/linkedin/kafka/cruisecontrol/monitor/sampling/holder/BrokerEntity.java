/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;


/**
 * The {@link Entity} class used by {@link MetricSampleAggregator} for brokers.
 */
public class BrokerEntity extends Entity<String> {
  private final String _host;
  private final int _brokerId;

  public BrokerEntity(String host, int brokerId) {
    _host = host;
    _brokerId = brokerId;
  }

  public int brokerId() {
    return _brokerId;
  }

  public String host() {
    return _host;
  }

  @Override
  public String group() {
    return _host;
  }

  @Override
  public int hashCode() {
    return _brokerId;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof BrokerEntity && ((BrokerEntity) other)._brokerId == _brokerId;
  }

  @Override
  public String toString() {
    return String.format("brokerId=%d,host=%s", _brokerId, _host);
  }
}
