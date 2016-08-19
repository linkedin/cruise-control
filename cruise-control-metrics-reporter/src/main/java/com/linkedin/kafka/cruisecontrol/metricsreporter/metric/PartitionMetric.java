/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.TopicPartition;


public class PartitionMetric extends TopicMetric {
  private static final byte METRIC_VERSION = 0;
  private final int _partition;

  public PartitionMetric(MetricType metricType, long time, int brokerId, String topic, int partition, double value) {
    super(metricType, time, brokerId, topic, value);
    _partition = partition;
  }

  public MetricClassId metricClassId() {
    return MetricClassId.PARTITION_METRIC;
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
    buffer.put(metricType().id());
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
    MetricType metricType = MetricType.forId(buffer.get());
    long time = buffer.getLong();
    int brokerId = buffer.getInt();
    int topicLength = buffer.getInt();
    String topic = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), topicLength, StandardCharsets.UTF_8);
    buffer.position(buffer.position() + topicLength);
    int partition = buffer.getInt();
    double value = buffer.getDouble();
    return new PartitionMetric(metricType, time, brokerId, topic, partition, value);
  }

  @Override
  public String toString() {
    return String.format("[%s,%s,time=%d,brokerId=%d,partition=%s,value=%.3f]",
                         MetricClassId.PARTITION_METRIC, metricType(), time(), brokerId(),
                         new TopicPartition(topic(), partition()), value());
  }
}
