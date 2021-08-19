/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.*;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;


/**
 * The unit test for {@link PartitionMetricSample}
 */
public class PartitionMetricSampleTest {
  private static final double EPSILON = 1E-6;
  @Test
  public void testRecordAfterClose() {
    PartitionMetricSample sample = new PartitionMetricSample(0, new TopicPartition("topic", 0));
    sample.close(0);
    try {
      sample.record(KafkaMetricDef.commonMetricDefInfo(DISK_USAGE), 0.0);
      fail("Should throw IllegalStateException");
    } catch (IllegalStateException ise) {
      // let it go.
    }
  }

  @Test
  public void testRecordSameResourceMetricAgain() {
    PartitionMetricSample sample = new PartitionMetricSample(0, new TopicPartition("topic", 0));
    sample.record(KafkaMetricDef.commonMetricDefInfo(DISK_USAGE), 0);
    try {
      sample.record(KafkaMetricDef.commonMetricDefInfo(DISK_USAGE), 0.0);
      fail("Should throw IllegalStateException");
    } catch (IllegalStateException ise) {
      // let it go.
    }
  }

  @Test
  public void testSerde() throws UnknownVersionException {
    MetricDef metricDef = KafkaMetricDef.commonMetricDef();
    PartitionMetricSample sample = new PartitionMetricSample(0, new TopicPartition("topic", 0));
    int i = 0;
    for (Resource r : Resource.cachedValues()) {
      sample.record(KafkaMetricDef.resourceToMetricInfo(r).get(0), i);
      i++;
    }
    sample.record(metricDef.metricInfo(PRODUCE_RATE.name()), i++);
    sample.record(metricDef.metricInfo(FETCH_RATE.name()), i++);
    sample.record(metricDef.metricInfo(MESSAGE_IN_RATE.name()), i++);
    sample.record(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()), i++);
    sample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), i);
    sample.close(10);
    byte[] bytes = sample.toBytes();
    PartitionMetricSample deserializedSample = PartitionMetricSample.fromBytes(bytes);
    assertEquals(sample.brokerId(), deserializedSample.brokerId());
    assertEquals(sample.entity().tp(), deserializedSample.entity().tp());
    assertEquals(sample.metricValue(KafkaMetricDef.resourceToMetricIds(Resource.CPU).get(0)),
                 deserializedSample.metricValue(KafkaMetricDef.resourceToMetricIds(Resource.CPU).get(0)));
    assertEquals(sample.metricValue(KafkaMetricDef.resourceToMetricIds(Resource.DISK).get(0)),
                 deserializedSample.metricValue(KafkaMetricDef.resourceToMetricIds(Resource.DISK).get(0)));
    assertEquals(sample.metricValue(KafkaMetricDef.resourceToMetricIds(Resource.NW_IN).get(0)),
                 deserializedSample.metricValue(KafkaMetricDef.resourceToMetricIds(Resource.NW_IN).get(0)));
    assertEquals(sample.metricValue(KafkaMetricDef.resourceToMetricIds(Resource.NW_OUT).get(0)),
                 deserializedSample.metricValue(KafkaMetricDef.resourceToMetricIds(Resource.NW_OUT).get(0)));
    assertEquals(sample.metricValue(metricDef.metricInfo(PRODUCE_RATE.name()).id()),
                 deserializedSample.metricValue(metricDef.metricInfo(PRODUCE_RATE.name()).id()), EPSILON);
    assertEquals(sample.metricValue(metricDef.metricInfo(FETCH_RATE.name()).id()),
                 deserializedSample.metricValue(metricDef.metricInfo(FETCH_RATE.name()).id()), EPSILON);
    assertEquals(sample.metricValue(metricDef.metricInfo(MESSAGE_IN_RATE.name()).id()),
                 deserializedSample.metricValue(metricDef.metricInfo(MESSAGE_IN_RATE.name()).id()), EPSILON);
    assertEquals(sample.metricValue(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()).id()),
                 deserializedSample.metricValue(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()).id()), EPSILON);
    assertEquals(sample.metricValue(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()).id()),
                 deserializedSample.metricValue(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()).id()), EPSILON);
    assertEquals(sample.sampleTime(), deserializedSample.sampleTime());
  }

}
