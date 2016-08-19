/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

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
      sample.record(Resource.DISK, 0.0);
      fail("Should throw IllegalStateException");
    } catch (IllegalStateException ise) {
      // let it go.
    }
  }

  @Test
  public void testRecordSameResourceMetricAgain() {
    PartitionMetricSample sample = new PartitionMetricSample(0, new TopicPartition("topic", 0));
    sample.record(Resource.DISK, 0);
    try {
      sample.record(Resource.DISK, 0.0);
      fail("Should throw IllegalStateException");
    } catch (IllegalStateException ise) {
      // let it go.
    }
  }

  @Test
  public void testSerde() throws UnknownVersionException {
    PartitionMetricSample sample = new PartitionMetricSample(0, new TopicPartition("topic", 0));
    int i = 0;
    for (Resource r : Resource.values()) {
      sample.record(r, i);
      i++;
    }
    sample.recordProduceRequestRate((double) i++);
    sample.recordFetchRequestRate((double) i++);
    sample.recordMessagesInPerSec((double) i++);
    sample.recordReplicationBytesInPerSec((double) i++);
    sample.recordReplicationBytesOutPerSec((double) i);
    sample.close(10);
    byte[] bytes = sample.toBytes();
    PartitionMetricSample deserializedSample = PartitionMetricSample.fromBytes(bytes);
    assertEquals(sample.brokerId(), deserializedSample.brokerId());
    assertEquals(sample.topicPartition(), deserializedSample.topicPartition());
    assertEquals(sample.metricFor(Resource.CPU), deserializedSample.metricFor(Resource.CPU));
    assertEquals(sample.metricFor(Resource.DISK), deserializedSample.metricFor(Resource.DISK));
    assertEquals(sample.metricFor(Resource.NW_IN), deserializedSample.metricFor(Resource.NW_IN));
    assertEquals(sample.metricFor(Resource.NW_OUT), deserializedSample.metricFor(Resource.NW_OUT));
    assertEquals(sample.produceRequestRate(), deserializedSample.produceRequestRate(), EPSILON);
    assertEquals(sample.fetchRequestRate(), deserializedSample.fetchRequestRate(), EPSILON);
    assertEquals(sample.messagesInPerSec(), deserializedSample.messagesInPerSec(), EPSILON);
    assertEquals(sample.replicationBytesInPerSec(), deserializedSample.replicationBytesInPerSec(), EPSILON);
    assertEquals(sample.replicationBytesOutPerSec(), deserializedSample.replicationBytesOutPerSec(), EPSILON);
    assertEquals(sample.sampleTime(), deserializedSample.sampleTime());
  }

}
