/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import java.nio.ByteBuffer;


/**
 * An interface for all the metrics reported by {@link CruiseControlMetricsReporter}.
 */
public interface CruiseControlMetric {

  /**
   * Get the metric class id for this metric. The metric class id will be stored in the serailized metrics
   * so that the deserializer will know which class should be used to deserialize the data.
   */
  MetricClassId metricClassId();

  /**
   * Get the {@link MetricType} of this metric.
   */
  MetricType metricType();

  /**
   * Get the timestamp for this metric.
   */
  long time();

  /**
   * Get the broker id who reported this metric.
   */
  int brokerId();

  /**
   * Get the metric value.
   */
  double value();

  /**
   * Serialize the metric to a byte buffer with the header size reserved.
   * @param headerSize the header size to reserve.
   * @return A ByteBuffer with header size reserved at the beginning.
   */
  ByteBuffer toBuffer(int headerSize);

  /**
   * An enum that list all the implementations of the interface. This id will be store in the serialized
   * metrics to help the metric sampler to decide using which class to deserialize the metric bytes.
   */
  enum MetricClassId {
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
}
