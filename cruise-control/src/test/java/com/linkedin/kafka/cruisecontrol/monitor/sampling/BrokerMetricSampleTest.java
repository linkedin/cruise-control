/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.util.regex.Pattern;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BrokerMetricSampleTest {
  private static final double DELTA = 1E-6;
  @Test
  public void testSerde() throws UnknownVersionException {
    BrokerMetricSample sample =
        new BrokerMetricSample(0,
                               1.0,
                               2.0,
                               3.0,
                               4.0,
                               5.0,
                               6.0,
                               7.0,
                               8.0,
                               9.0,
                               10.0,
                               11.0,
                               12.0,
                               13.0,
                               1000L,
                               14,
                               15,
                               16,
                               17,
                               18,
                               19,
                               20,
                               21,
                               22.0,
                               23.0,
                               24.0,
                               25.0,
                               26.0,
                               27.0,
                               28.0,
                               29.0,
                               30.0);
    System.out.println(sample);
    byte[] bytes = sample.toBytes();
    BrokerMetricSample deserializedSample = BrokerMetricSample.fromBytes(bytes);
    assertEquals(0, deserializedSample.brokerId());
    assertEquals(1.0, deserializedSample.brokerCpuUtil(), DELTA);
    assertEquals(2.0, deserializedSample.brokerLeaderBytesInRate(), DELTA);
    assertEquals(3.0, deserializedSample.brokerLeaderBytesOutRate(), DELTA);
    assertEquals(4.0, deserializedSample.brokerReplicationBytesInRate(), DELTA);
    assertEquals(5.0, deserializedSample.brokerReplicationBytesOutRate(), DELTA);
    assertEquals(6.0, deserializedSample.brokerMessagesInRate(), DELTA);
    assertEquals(7.0, deserializedSample.brokerProduceRequestRate(), DELTA);
    assertEquals(8.0, deserializedSample.brokerConsumerFetchRequestRate(), DELTA);
    assertEquals(9.0, deserializedSample.brokerReplicationFetchRequestRate(), DELTA);
    assertEquals(10.0, deserializedSample.brokerRequestHandlerAvgIdlePercent(), DELTA);
    assertEquals(11.0, deserializedSample.brokerDiskUtilization(), DELTA);
    assertEquals(12.0, deserializedSample.allTopicsProduceRequestRate(), DELTA);
    assertEquals(13.0, deserializedSample.allTopicsFetchRequestRate(), DELTA);
    assertEquals(1000, deserializedSample.sampleTime());
    assertEquals(14, deserializedSample.requestQueueSize());
    assertEquals(15, deserializedSample.responseQueueSize());
    assertEquals(16, deserializedSample.produceRequestQueueTimeMsMax(), DELTA);
    assertEquals(17, deserializedSample.produceRequestQueueTimeMsMean(), DELTA);
    assertEquals(18, deserializedSample.consumerFetchRequestQueueTimeMsMax(), DELTA);
    assertEquals(19, deserializedSample.consumerFetchRequestQueueTimeMsMean(), DELTA);
    assertEquals(20, deserializedSample.followerFetchRequestQueueTimeMsMax(), DELTA);
    assertEquals(21, deserializedSample.followerFetchRequestQueueTimeMsMean(), DELTA);
    assertEquals(22.0, deserializedSample.produceTotalTimeMsMax(), DELTA);
    assertEquals(23.0, deserializedSample.produceTotalTimeMsMean(), DELTA);
    assertEquals(24.0, deserializedSample.consumerFetchTotalTimeMsMax(), DELTA);
    assertEquals(25.0, deserializedSample.consumerFetchTotalTimeMsMean(), DELTA);
    assertEquals(26.0, deserializedSample.followerFetchTotalTimeMsMax(), DELTA);
    assertEquals(27.0, deserializedSample.followerFetchTotalTimeMsMean(), DELTA);
    assertEquals(28.0, deserializedSample.logFlushRate(), DELTA);
    assertEquals(29.0, deserializedSample.logFlushTimeMaxMs(), DELTA);
    assertEquals(30.0, deserializedSample.logFlushTimeMeanMs(), DELTA);
  }

  @Test
  public void patterTest() {
    Pattern pattern = Pattern.compile("topic1|.*aaa.*");
    assertTrue(pattern.matcher("topic1").matches());
    assertTrue(pattern.matcher("bbaaask").matches());

    pattern = Pattern.compile("");
    assertFalse(pattern.matcher("sf").matches());
  }
}
