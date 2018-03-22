/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.regex.Pattern;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BrokerMetricSampleTest {
  private static final double DELTA = 1E-6;
  @Test
  public void testSerde() throws UnknownVersionException {
    MetricDef brokerMetricDef = KafkaMetricDef.brokerMetricDef();
    BrokerMetricSample sample = new BrokerMetricSample("host", 0);
    double value = 1.0;
    for (MetricInfo metricInfo : brokerMetricDef.all()) {
      sample.record(metricInfo, value);
      value += 1;
    }
    sample.close((long) value);
    byte[] bytes = sample.toBytes();
    BrokerMetricSample deserializedSample = BrokerMetricSample.fromBytes(bytes);

    assertEquals("host", deserializedSample.entity().host());
    assertEquals(0, deserializedSample.entity().brokerId());

    value = 1.0;
    for (MetricInfo metricInfo : brokerMetricDef.all()) {
      assertEquals(value, deserializedSample.metricValue(metricInfo.id()), 0.0);
      value += 1;
    }
    assertEquals(value, deserializedSample.sampleTime(), 0.0);
  }

  @Test
  public void patternTest() {
    Pattern pattern = Pattern.compile("topic1|.*aaa.*");
    assertTrue(pattern.matcher("topic1").matches());
    assertTrue(pattern.matcher("bbaaask").matches());

    pattern = Pattern.compile("");
    assertFalse(pattern.matcher("sf").matches());
  }
}
