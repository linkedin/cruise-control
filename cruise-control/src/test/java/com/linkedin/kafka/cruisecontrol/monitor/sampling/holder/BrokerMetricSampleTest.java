/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BrokerMetricSampleTest {
  @Test
  public void testSerdeWithLatestSerdeVersion() throws UnknownVersionException {
    MetricDef brokerMetricDef = KafkaMetricDef.brokerMetricDef();
    BrokerMetricSample sample = new BrokerMetricSample("host", 0, BrokerMetricSample.LATEST_SUPPORTED_VERSION);
    double value = 1.0;
    for (MetricInfo metricInfo : brokerMetricDef.all()) {
      sample.record(metricInfo, value);
      value += 1;
    }
    sample.close((long) value);
    byte[] bytes = sample.toBytes();
    assertEquals(461, bytes.length);
    BrokerMetricSample deserializedSample = BrokerMetricSample.fromBytes(bytes);

    assertEquals("host", deserializedSample.entity().host());
    assertEquals(0, deserializedSample.entity().brokerId());
    assertEquals(BrokerMetricSample.LATEST_SUPPORTED_VERSION, deserializedSample.deserializationVersion());

    // Disk usage is not one of the broker raw metric type so we add 1.
    int expectedNumRecords = 1;
    expectedNumRecords += RawMetricType.brokerMetricTypesDiffByVersion().values().stream()
                                       .mapToInt(Set::size).sum();

    assertEquals(expectedNumRecords, deserializedSample.allMetricValues().size());

    value = 1.0;
    for (MetricInfo metricInfo : brokerMetricDef.all()) {
      assertEquals(value, deserializedSample.metricValue(metricInfo.id()), 0.0);
      value += 1;
    }
    assertEquals(value, deserializedSample.sampleTime(), 0.0);
  }

  @Test
  public void testSerdeWithOldSerdeVersion() throws UnknownVersionException {
    MetricDef brokerMetricDef = KafkaMetricDef.brokerMetricDef();
    BrokerMetricSample sample = new BrokerMetricSample("host", 0, BrokerMetricSample.MIN_SUPPORTED_VERSION);
    double value = 1.0;
    for (MetricInfo metricInfo : brokerMetricDef.all()) {
      sample.record(metricInfo, value);
      value += 1;
    }
    sample.close((long) value);
    byte[] bytes = sample.toBytes();
    assertEquals(461, bytes.length);
    BrokerMetricSample deserializedSample = BrokerMetricSample.fromBytes(bytes);

    assertEquals("host", deserializedSample.entity().host());
    assertEquals(0, deserializedSample.entity().brokerId());
    assertEquals(BrokerMetricSample.MIN_SUPPORTED_VERSION, deserializedSample.deserializationVersion());

    // Disk usage is not one of the broker raw metric type so we add 1.
    int expectedNumRecords = RawMetricType.brokerMetricTypesDiffForVersion(BrokerMetricSample.MIN_SUPPORTED_VERSION).size() + 1;
    assertEquals(expectedNumRecords, deserializedSample.allMetricValues().size());

    value = 1.0;
    for (MetricInfo metricInfo : brokerMetricDef.all()) {
      assertEquals(value, deserializedSample.metricValue(metricInfo.id()), 0.0);
      value += 1;
      if (value > expectedNumRecords) {
        break;
      }
    } assertNotEquals(value, deserializedSample.sampleTime(), 0.0);
  }

  @Test
  public void testPattern() {
    Pattern pattern = Pattern.compile("topic1|.*aaa.*");
    assertTrue(pattern.matcher("topic1").matches());
    assertTrue(pattern.matcher("bbaaask").matches());

    pattern = Pattern.compile("");
    assertFalse(pattern.matcher("sf").matches());
  }
}
