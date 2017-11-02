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
                               1000L);
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
