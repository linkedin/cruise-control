/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import java.nio.ByteBuffer;


/**
 * An interface for all the raw metrics reported by {@link CruiseControlMetricsReporter}.
 */
public abstract class CruiseControlMetric {
  static final byte METRIC_VERSION = 0;
  private final RawMetricType _rawMetricType;
  private final long _time;
  private final int _brokerId;
  private final double _value;

  public CruiseControlMetric(RawMetricType rawMetricType, long time, int brokerId, double value) {
    _rawMetricType = rawMetricType;
    _time = time;
    _brokerId = brokerId;
    _value = value;
  }

  /**
   * @return the metric class id for this metric. The metric class id will be stored in the serialized metrics
   * so that the deserializer will know which class should be used to deserialize the data.
   */
  public abstract MetricClassId metricClassId();

  /**
   * @return the {@link RawMetricType} of this metric.
   */
  public RawMetricType rawMetricType() {
    return _rawMetricType;
  }

  /**
   * @return the timestamp for this metric.
   */
  public long time() {
    return _time;
  }

  /**
   * @return the broker id who reported this metric.
   */
  public int brokerId() {
    return _brokerId;
  }

  /**
   * @return the metric value.
   */
  public double value() {
    return _value;
  }

  /**
   * Serialize the metric to a byte buffer with the header size reserved.
   * @param headerSize the header size to reserve.
   * @return A ByteBuffer with header size reserved at the beginning.
   */
  abstract ByteBuffer toBuffer(int headerSize);

  /**
   * An enum that list all the implementations of the interface. This id will be store in the serialized
   * metrics to help the metric sampler to decide using which class to deserialize the metric bytes.
   */
  public enum MetricClassId {
    BROKER_METRIC((byte) 0), TOPIC_METRIC((byte) 1), PARTITION_METRIC((byte) 2);

    private final byte _id;

    MetricClassId(byte id) {
      _id = id;
    }

    byte id() {
      return _id;
    }

    static MetricClassId forId(byte id) {
      if (id < values().length) {
        return values()[id];
      } else {
        throw new IllegalArgumentException("MetricClassId " + id + " does not exist.");
      }
    }
  }

  @Override
  public String toString() {
    return String.format("[RawMetricType=%s,Time=%d,BrokerId=%d,Value=%.4f]", _rawMetricType, _time, _brokerId, _value);
  }
}
