/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit tests for the additions to {@link MetricsUtils} that recognise the per-listener
 * socket-server metrics. The tests live next to existing reporter metric tests and are
 * deliberately narrow: they exercise the filter (isInterested) and the small public helpers
 * the reporter uses to identify socket-server gauges at emission time.
 */
public class MetricsUtilsTest {
  private static MetricName name(String name, String group, Map<String, String> tags) {
    return new MetricName(name, group, "", tags);
  }

  @Test
  public void testIsInterestedAcceptsSocketServerConnectionCount() {
    MetricName mn = name("connection-count", "socket-server-metrics", Map.of("listener", "PLAINTEXT"));
    assertTrue("connection-count on socket-server-metrics group must be interesting",
        MetricsUtils.isInterested(mn));
  }

  @Test
  public void testIsInterestedAcceptsSocketServerMaxConnections() {
    MetricName mn = name("max-connections", "socket-server-metrics", Map.of("listener", "PLAINTEXT"));
    assertTrue("max-connections on socket-server-metrics group must be interesting",
        MetricsUtils.isInterested(mn));
  }

  @Test
  public void testIsInterestedRejectsOtherSocketServerMetrics() {
    MetricName mn = name("connection-creation-rate", "socket-server-metrics", Collections.emptyMap());
    assertFalse("Only connection-count and max-connections are consumed today",
        MetricsUtils.isInterested(mn));
  }

  @Test
  public void testIsSocketServerConnectionCountHelper() {
    MetricName conn = name("connection-count", "socket-server-metrics", Collections.emptyMap());
    assertTrue(MetricsUtils.isSocketServerConnectionCount(conn));

    MetricName other = name("max-connections", "socket-server-metrics", Collections.emptyMap());
    assertFalse(MetricsUtils.isSocketServerConnectionCount(other));

    MetricName wrongGroup = name("connection-count", "kafka.server", Collections.emptyMap());
    assertFalse(MetricsUtils.isSocketServerConnectionCount(wrongGroup));
  }

  @Test
  public void testIsSocketServerMaxConnectionsHelper() {
    MetricName maxConn = name("max-connections", "socket-server-metrics", Collections.emptyMap());
    assertTrue(MetricsUtils.isSocketServerMaxConnections(maxConn));

    MetricName other = name("connection-count", "socket-server-metrics", Collections.emptyMap());
    assertFalse(MetricsUtils.isSocketServerMaxConnections(other));
  }
}
