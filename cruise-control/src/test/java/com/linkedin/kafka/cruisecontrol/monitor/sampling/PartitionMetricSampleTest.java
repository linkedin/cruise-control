/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaCruiseControlMetricDef;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaCruiseControlMetricDef.*;
import static org.junit.Assert.*;


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
      sample.record(KafkaCruiseControlMetricDef.resourceToMetricInfo(Resource.DISK), 0.0);
      fail("Should throw IllegalStateException");
    } catch (IllegalStateException ise) {
      // let it go.
    }
  }

  @Test
  public void testRecordSameResourceMetricAgain() {
    PartitionMetricSample sample = new PartitionMetricSample(0, new TopicPartition("topic", 0));
    sample.record(KafkaCruiseControlMetricDef.resourceToMetricInfo(Resource.DISK), 0);
    try {
      sample.record(KafkaCruiseControlMetricDef.resourceToMetricInfo(Resource.DISK), 0.0);
      fail("Should throw IllegalStateException");
    } catch (IllegalStateException ise) {
      // let it go.
    }
  }

  @Test
  public void testSerde() throws UnknownVersionException {
    MetricDef metricDef = KafkaCruiseControlMetricDef.metricDef();
    PartitionMetricSample sample = new PartitionMetricSample(0, new TopicPartition("topic", 0));
    int i = 0;
    for (Resource r : Resource.values()) {
      sample.record(KafkaCruiseControlMetricDef.resourceToMetricInfo(r), i);
      i++;
    }
    sample.record(metricDef.metricInfo(PRODUCE_RATE.name()), (double) i++);
    sample.record(metricDef.metricInfo(FETCH_RATE.name()), (double) i++);
    sample.record(metricDef.metricInfo(MESSAGE_IN_RATE.name()), (double) i++);
    sample.record(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()), (double) i++);
    sample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), (double) i);
    sample.close(10);
    byte[] bytes = sample.toBytes();
    PartitionMetricSample deserializedSample = PartitionMetricSample.fromBytes(bytes);
    assertEquals(sample.brokerId(), deserializedSample.brokerId());
    assertEquals(sample.entity().tp(), deserializedSample.entity().tp());
    assertEquals(sample.metricValue(KafkaCruiseControlMetricDef.resourceToMetricId(Resource.CPU)),
                 deserializedSample.metricValue(KafkaCruiseControlMetricDef.resourceToMetricId(Resource.CPU)));
    assertEquals(sample.metricValue(KafkaCruiseControlMetricDef.resourceToMetricId(Resource.DISK)),
                 deserializedSample.metricValue(KafkaCruiseControlMetricDef.resourceToMetricId(Resource.DISK)));
    assertEquals(sample.metricValue(KafkaCruiseControlMetricDef.resourceToMetricId(Resource.NW_IN)),
                 deserializedSample.metricValue(KafkaCruiseControlMetricDef.resourceToMetricId(Resource.NW_IN)));
    assertEquals(sample.metricValue(KafkaCruiseControlMetricDef.resourceToMetricId(Resource.NW_OUT)),
                 deserializedSample.metricValue(KafkaCruiseControlMetricDef.resourceToMetricId(Resource.NW_OUT)));
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
