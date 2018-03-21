/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


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
}
