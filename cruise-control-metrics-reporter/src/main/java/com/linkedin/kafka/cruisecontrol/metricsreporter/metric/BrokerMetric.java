/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;


/**
 * A container class to hold broker metric.
 */
public class BrokerMetric implements CruiseControlMetric {
  private static final byte METRIC_VERSION = 0;
  private final MetricType _metricType;
  private final long _time;
  private final int _brokerId;
  private final double _value;

  public BrokerMetric(MetricType metricType, long time, int brokerId, double value) {
    _metricType = metricType;
    _time = time;
    _brokerId = brokerId;
    _value = value;
  }

  @Override
  public MetricClassId metricClassId() {
    return MetricClassId.BROKER_METRIC;
  }

  @Override
  public MetricType metricType() {
    return _metricType;
  }

  @Override
  public long time() {
    return _time;
  }

  @Override
  public int brokerId() {
    return _brokerId;
  }

  @Override
  public double value() {
    return _value;
  }

  @Override
  public ByteBuffer toBuffer(int headerPos) {
    ByteBuffer buffer = ByteBuffer.allocate(headerPos + 1 /* version */ + 1 /* metric type */ +
                                                Long.BYTES /* time */ + Integer.BYTES /* broker id */ +
                                                Double.BYTES /* value */);
    buffer.position(headerPos);
    buffer.put(METRIC_VERSION);
    buffer.put(metricType().id());
    buffer.putLong(time());
    buffer.putInt(brokerId());
    buffer.putDouble(value());
    return buffer;
  }

  static BrokerMetric fromBuffer(ByteBuffer buffer) throws UnknownVersionException {
    byte version = buffer.get();
    if (version > METRIC_VERSION) {
      throw new UnknownVersionException("Cannot deserialize the topic metrics for version " + version + ". "
                                            + "Current version is " + METRIC_VERSION);
    }
    MetricType metricType = MetricType.forId(buffer.get());
    long time = buffer.getLong();
    int brokerId = buffer.getInt();
    double value = buffer.getDouble();
    return new BrokerMetric(metricType, time, brokerId, value);
  }

  @Override
  public String toString() {
    return String.format("[%s,%s,time=%d,brokerId=%d,value=%.3f]",
                         MetricClassId.BROKER_METRIC, metricType(), time(), brokerId(), value());
  }
}

