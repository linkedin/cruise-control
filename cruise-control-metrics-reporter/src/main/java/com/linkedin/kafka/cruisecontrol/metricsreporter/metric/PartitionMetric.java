/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.TopicPartition;


public class PartitionMetric extends CruiseControlMetric {
  private static final byte METRIC_VERSION = 0;
  private final String _topic;
  private final int _partition;

  public PartitionMetric(RawMetricType rawMetricType, long time, int brokerId, String topic, int partition, double value) {
    super(rawMetricType, time, brokerId, value);
    if (rawMetricType.metricScope() != RawMetricType.MetricScope.PARTITION) {
      throw new IllegalArgumentException(String.format("Cannot construct a PartitionMetric for %s whose scope is %s",
                                                       rawMetricType, rawMetricType.metricScope()));
    }
    _topic = topic;
    _partition = partition;
  }

  public MetricClassId metricClassId() {
    return MetricClassId.PARTITION_METRIC;
  }

  public String topic() {
    return _topic;
  }

  public int partition() {
    return _partition;
  }

  public ByteBuffer toBuffer(int headerPos) {
    byte[] topic = topic().getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(headerPos + 1 /* version */ + 1 /* metric type */ +
                                                Long.BYTES /* time */ + Integer.BYTES /* broker id */ +
                                                Integer.BYTES /* topic length */ + topic.length /* topic */ +
                                                Integer.BYTES /* partition */ + Double.BYTES /* value */);
    buffer.position(headerPos);
    buffer.put(METRIC_VERSION);
    buffer.put(rawMetricType().id());
    buffer.putLong(time());
    buffer.putInt(brokerId());
    buffer.putInt(topic.length);
    buffer.put(topic);
    buffer.putInt(_partition);
    buffer.putDouble(value());
    return buffer;
  }

  static PartitionMetric fromBuffer(ByteBuffer buffer) throws UnknownVersionException {
    byte version = buffer.get();
    if (version > METRIC_VERSION) {
      throw new UnknownVersionException("Cannot deserialize the topic metrics for version " + version + ". "
                                            + "Current version is " + METRIC_VERSION);
    }
    RawMetricType rawMetricType = RawMetricType.forId(buffer.get());
    long time = buffer.getLong();
    int brokerId = buffer.getInt();
    int topicLength = buffer.getInt();
    String topic = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), topicLength, StandardCharsets.UTF_8);
    buffer.position(buffer.position() + topicLength);
    int partition = buffer.getInt();
    double value = buffer.getDouble();
    return new PartitionMetric(rawMetricType, time, brokerId, topic, partition, value);
  }

  @Override
  public String toString() {
    return String.format("[%s,%s,time=%d,brokerId=%d,partition=%s,value=%.3f]",
                         MetricClassId.PARTITION_METRIC, rawMetricType(), time(), brokerId(),
                         new TopicPartition(topic(), partition()), value());
  }
}
