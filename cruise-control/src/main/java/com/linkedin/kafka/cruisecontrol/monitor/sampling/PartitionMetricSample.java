/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.resource.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.TopicPartition;

import java.util.Date;
import java.util.Map;

import static java.nio.charset.StandardCharsets.*;


/**
 * The class that hosts one the metrics samples of the following resources:
 * CPU, DISK, Network Bytes In, Network Bytes Out.
 */
public class PartitionMetricSample extends MetricSample<TopicPartition> {
  private static final byte CURRENT_VERSION = 1;

  private final int _brokerId;
  private double _produceRequestRate = 0.0;
  private double _fetchRequestRate = 0.0;
  private double _messagesInPerSec = 0.0;
  private double _replicationBytesInPerSec = 0.0;
  private double _replicationBytesOutPerSec = 0.0;

  public PartitionMetricSample(int brokerId, TopicPartition tp) {
    super(tp);
    _brokerId = brokerId;
  }

  /**
   * Set the produce request rate
   */
  void recordProduceRequestRate(double produceRequestRate) {
    _produceRequestRate = produceRequestRate;
  }

  /**
   * Set the fetch request rate
   */
  void recordFetchRequestRate(double fetchRequestRate) {
    _fetchRequestRate = fetchRequestRate;
  }

  /**
   * Set the replication bytes in rate
   */
  void recordReplicationBytesInPerSec(double replicationBytesInPerSec) {
    _replicationBytesInPerSec = replicationBytesInPerSec;
  }

  /**
   * Set the replication bytes out rate
   */
  void recordReplicationBytesOutPerSec(double replicationBytesOutPerSec) {
    _replicationBytesOutPerSec = replicationBytesOutPerSec;
  }

  /**
   * Set the messages in per sec.
   */
  void recordMessagesInPerSec(double messagesInPerSec) {
    _messagesInPerSec = messagesInPerSec;
  }

  /**
   * The id of the broker from which the metrics are from.
   */
  public int brokerId() {
    return _brokerId;
  }

  /**
   * The produce request rate for this partition.
   */
  public double produceRequestRate() {
    return _produceRequestRate;
  }

  /**
   * The fetch request rate for this partition.
   */
  public double fetchRequestRate() {
    return _fetchRequestRate;
  }

  /**
   * The replication bytes in rate for this partition
   */
  public double replicationBytesInPerSec() {
    return _replicationBytesInPerSec;
  }

  /**
   * The replication bytes out rate for this partition
   */
  public double replicationBytesOutPerSec() {
    return _replicationBytesOutPerSec;
  }

  /**
   * The messages in rate for this partition
   */
  public double messagesInPerSec() {
    return _messagesInPerSec;
  }

  /**
   * Give the number or metrics that has been recorded.
   */
  public int numMetrics() {
    return _metrics.size();
  }

  /**
   * This method serialize the metric sample using a simple protocol.
   * 1 byte  - version
   * 4 bytes - brokerId
   * 8 bytes - CPU Utilization
   * 8 bytes - DISK Utilization
   * 8 bytes - Network Inbound Utilization
   * 8 bytes - Network Outbound Utilization.
   * 8 bytes - Produce Request Rate
   * 8 bytes - Fetch Request Rate
   * 8 bytes - Messages In Per Sec
   * 8 bytes - Replication Bytes In Per Sec
   * 8 bytes - Replication Bytes Out Per Sec
   * 8 bytes - Sample time
   * 4 bytes - partition id
   * N bytes - topic string bytes
   */
  public byte[] toBytes() {
    byte[] topicStringBytes = entity().topic().getBytes(UTF_8);
    // Allocate memory:
    ByteBuffer buffer = ByteBuffer.allocate(89 + topicStringBytes.length);
    buffer.put(CURRENT_VERSION);
    buffer.putInt(_brokerId);
    buffer.putDouble(_metrics.get(Resource.CPU));
    buffer.putDouble(_metrics.get(Resource.DISK));
    buffer.putDouble(_metrics.get(Resource.NW_IN));
    buffer.putDouble(_metrics.get(Resource.NW_OUT));
    buffer.putDouble(_produceRequestRate);
    buffer.putDouble(_fetchRequestRate);
    buffer.putDouble(_messagesInPerSec);
    buffer.putDouble(_replicationBytesInPerSec);
    buffer.putDouble(_replicationBytesOutPerSec);
    buffer.putLong(_sampleTime);
    buffer.putInt(entity().partition());
    buffer.put(topicStringBytes);
    return buffer.array();
  }

  public static PartitionMetricSample fromBytes(byte[] bytes) throws UnknownVersionException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    // Not used at this point.
    byte version = buffer.get();
    if (version > CURRENT_VERSION) {
      throw new UnknownVersionException("Metric sample version " + version +
          " is higher than current version " + CURRENT_VERSION);
    }
    switch (version) {
      case 0:
        return readV0(buffer);
      case 1:
        return readV1(buffer);
      default:
        throw new IllegalStateException("Should never be here.");
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder().append("{");
    for (Map.Entry<Resource, Double> entry : _metrics.entrySet()) {
      builder.append(entry.getKey().toString())
          .append("=")
          .append(entry.getValue().toString())
          .append(", ");
    }
    builder.delete(builder.length() - 2, builder.length()).append("}");
    return String.format("[brokerId: %d, Partition: %s, time: %s, metrics: %s]", _brokerId, entity(),
        new Date(_sampleTime), builder.toString());
  }

  private static PartitionMetricSample readV0(ByteBuffer buffer) {
    int brokerId = buffer.getInt();
    int partition = buffer.getInt(45);
    String topic = new String(buffer.array(), 49, buffer.array().length - 49, UTF_8);
    PartitionMetricSample sample = new PartitionMetricSample(brokerId, new TopicPartition(topic, partition));
    sample.record(Resource.CPU, buffer.getDouble());
    sample.record(Resource.DISK, buffer.getDouble());
    sample.record(Resource.NW_IN, buffer.getDouble());
    sample.record(Resource.NW_OUT, buffer.getDouble());
    sample.close(buffer.getLong());
    return sample;
  }

  private static PartitionMetricSample readV1(ByteBuffer buffer) {
    int brokerId = buffer.getInt();
    int partition = buffer.getInt(85);
    String topic = new String(buffer.array(), 89, buffer.array().length - 89, UTF_8);
    PartitionMetricSample sample = new PartitionMetricSample(brokerId, new TopicPartition(topic, partition));
    sample.record(Resource.CPU, buffer.getDouble());
    sample.record(Resource.DISK, buffer.getDouble());
    sample.record(Resource.NW_IN, buffer.getDouble());
    sample.record(Resource.NW_OUT, buffer.getDouble());
    sample.recordProduceRequestRate(buffer.getDouble());
    sample.recordFetchRequestRate(buffer.getDouble());
    sample.recordMessagesInPerSec(buffer.getDouble());
    sample.recordReplicationBytesInPerSec(buffer.getDouble());
    sample.recordReplicationBytesOutPerSec(buffer.getDouble());
    sample.close(buffer.getLong());
    return sample;
  }
}
