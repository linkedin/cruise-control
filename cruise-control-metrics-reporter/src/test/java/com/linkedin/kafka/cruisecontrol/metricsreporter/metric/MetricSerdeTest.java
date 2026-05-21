/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class MetricSerdeTest {
  private static final long TIME = 123L;
  private static final int BROKER_ID = 0;
  private static final String TOPIC = "topic";
  private static final int PARTITION = 100;
  private static final double VALUE = 0.1;

  @Test
  public void testBrokerMetricSerde() throws UnknownVersionException {
    BrokerMetric brokerMetric = new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_IN, 123L, 0, 0.1);
    CruiseControlMetric deserialized = MetricSerde.fromBytes(MetricSerde.toBytes(brokerMetric));
    assertEquals(CruiseControlMetric.MetricClassId.BROKER_METRIC.id(), deserialized.metricClassId().id());
    assertEquals(RawMetricType.ALL_TOPIC_BYTES_IN.id(), deserialized.rawMetricType().id());
    assertEquals(TIME, deserialized.time());
    assertEquals(BROKER_ID, deserialized.brokerId());
    assertEquals(VALUE, deserialized.value(), 0.000001);
  }

  @Test
  public void testTopicMetricSerde() throws UnknownVersionException {
    TopicMetric topicMetric = new TopicMetric(RawMetricType.TOPIC_BYTES_IN, 123L, 0, TOPIC, 0.1);
    CruiseControlMetric deserialized = MetricSerde.fromBytes(MetricSerde.toBytes(topicMetric));
    assertEquals(CruiseControlMetric.MetricClassId.TOPIC_METRIC.id(), deserialized.metricClassId().id());
    assertEquals(RawMetricType.TOPIC_BYTES_IN.id(), deserialized.rawMetricType().id());
    assertEquals(TIME, deserialized.time());
    assertEquals(BROKER_ID, deserialized.brokerId());
    assertEquals(TOPIC, ((TopicMetric) deserialized).topic());
    assertEquals(VALUE, deserialized.value(), 0.000001);
  }

  @Test
  public void testPartitionMetricSerde() throws UnknownVersionException {
    PartitionMetric partitionMetric = new PartitionMetric(RawMetricType.PARTITION_SIZE, 123L, 0, TOPIC, PARTITION, 0.1);
    CruiseControlMetric deserialized = MetricSerde.fromBytes(MetricSerde.toBytes(partitionMetric));
    assertEquals(CruiseControlMetric.MetricClassId.PARTITION_METRIC.id(), deserialized.metricClassId().id());
    assertEquals(RawMetricType.PARTITION_SIZE.id(), deserialized.rawMetricType().id());
    assertEquals(TIME, deserialized.time());
    assertEquals(BROKER_ID, deserialized.brokerId());
    assertEquals(TOPIC, ((PartitionMetric) deserialized).topic());
    assertEquals(PARTITION, ((PartitionMetric) deserialized).partition());
    assertEquals(VALUE, deserialized.value(), 0.000001);
  }

  @Test
  public void testBrokerConnectionMetricsSerde() throws UnknownVersionException {
    BrokerMetric count = new BrokerMetric(RawMetricType.BROKER_CONNECTION_COUNT, TIME, BROKER_ID, 12345.0);
    CruiseControlMetric deserializedCount = MetricSerde.fromBytes(MetricSerde.toBytes(count));
    assertEquals(RawMetricType.BROKER_CONNECTION_COUNT.id(), deserializedCount.rawMetricType().id());
    assertEquals(12345.0, deserializedCount.value(), 0.000001);

    BrokerMetric capacity = new BrokerMetric(RawMetricType.BROKER_CONNECTION_CAPACITY, TIME, BROKER_ID, 500000.0);
    CruiseControlMetric deserializedCapacity = MetricSerde.fromBytes(MetricSerde.toBytes(capacity));
    assertEquals(RawMetricType.BROKER_CONNECTION_CAPACITY.id(), deserializedCapacity.rawMetricType().id());
    assertEquals(500000.0, deserializedCapacity.value(), 0.000001);
  }

  /**
   * Backward-compatibility contract: an older deserializer that does not know about a future
   * {@link RawMetricType} id must drop the sample rather than throw. We simulate that by writing
   * a broker metric whose raw-metric-id byte is past the end of the current enum.
   */
  @Test
  public void testForwardCompatibleUnknownRawMetricTypeIsDropped() throws UnknownVersionException {
    byte unknownRawMetricId = (byte) (RawMetricType.values().length + 5);
    ByteBuffer buffer = ByteBuffer.allocate(1 + 1 + 1 + Long.BYTES + Integer.BYTES + Double.BYTES);
    buffer.put(CruiseControlMetric.MetricClassId.BROKER_METRIC.id());
    // BrokerMetric METRIC_VERSION (0) — must match the current wire format.
    buffer.put((byte) 0);
    buffer.put(unknownRawMetricId);
    buffer.putLong(TIME);
    buffer.putInt(BROKER_ID);
    buffer.putDouble(VALUE);
    assertNull("Unknown raw metric ids must deserialize to null", MetricSerde.fromBytes(buffer.array()));
  }

  /**
   * Backward-compatibility contract for the outer class id: an unknown
   * {@link CruiseControlMetric.MetricClassId} byte must also drop cleanly.
   */
  @Test
  public void testForwardCompatibleUnknownMetricClassIdIsDropped() throws UnknownVersionException {
    byte unknownClassId = (byte) (CruiseControlMetric.MetricClassId.values().length + 5);
    byte[] payload = new byte[16];
    payload[0] = unknownClassId;
    assertNull("Unknown metric class ids must deserialize to null", MetricSerde.fromBytes(payload));
  }
}
