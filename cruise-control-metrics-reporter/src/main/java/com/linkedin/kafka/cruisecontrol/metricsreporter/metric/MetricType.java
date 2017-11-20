/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

/**
 * The metric type helps the metric sampler to distinguish what metric a value is representing.
 * Each metric type has an id for serde purpose.
 */
public enum MetricType {
  ALL_TOPIC_BYTES_IN((byte) 0),
  ALL_TOPIC_BYTES_OUT((byte) 1),
  TOPIC_BYTES_IN((byte) 2),
  TOPIC_BYTES_OUT((byte) 3),
  PARTITION_SIZE((byte) 4),
  BROKER_CPU_UTIL((byte) 5),
  ALL_TOPIC_REPLICATION_BYTES_IN((byte) 6),
  ALL_TOPIC_REPLICATION_BYTES_OUT((byte) 7),
  // Note that this is different from broker produce request rate. If one ProduceRequest produces to 3 partitions,
  // it would be counted as one ProduceRequest on the broker, but ALL_TOPIC_PRODUCE_REQUEST would increment by 3.
  // The multiplier is the number of the partitions in the produce request.
  ALL_TOPIC_PRODUCE_REQUEST_RATE((byte) 8),
  // Note that this is different from broker fetch request rate. If one FetchRequest fetches from 3 partitions,
  // it would be counted as one FetchRequest on the broker, but ALL_TOPIC_FETCH_REQUEST would increment by 3.
  // The multiplier is the number of the partitions in the fetch request.
  ALL_TOPIC_FETCH_REQUEST_RATE((byte) 9),
  ALL_TOPIC_MESSAGES_IN_PER_SEC((byte) 10),
  TOPIC_REPLICATION_BYTES_IN((byte) 11),
  TOPIC_REPLICATION_BYTES_OUT((byte) 12),
  TOPIC_PRODUCE_REQUEST_RATE((byte) 13),
  TOPIC_FETCH_REQUEST_RATE((byte) 14),
  TOPIC_MESSAGES_IN_PER_SEC((byte) 15),
  BROKER_PRODUCE_REQUEST_RATE((byte) 16),
  BROKER_CONSUMER_FETCH_REQUEST_RATE((byte) 17),
  BROKER_FOLLOWER_FETCH_REQUEST_RATE((byte) 18),
  BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT((byte) 19);

  private byte _id;

  MetricType(byte id) {
    _id = id;
  }

  public byte id() {
    return _id;
  }

  public static MetricType forId(byte id) {
    if (id < values().length) {
      return values()[id];
    } else {
      throw new IllegalArgumentException("CruiseControlMetric type " + id + " does not exist.");
    }
  }
}
