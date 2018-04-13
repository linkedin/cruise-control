/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit test for CruiseControlMetricsProcessor
 */
public class CruiseControlMetricsProcessorTest {
  private static final int BYTES_IN_KB = 1024;
  private static final int BYTES_IN_MB = 1024 * 1024;
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final int P0 = 0;
  private static final int P1 = 1;
  private static final TopicPartition T1P0 = new TopicPartition(TOPIC1, P0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC1, P1);
  private static final TopicPartition T2P0 = new TopicPartition(TOPIC2, P0);
  private static final TopicPartition T2P1 = new TopicPartition(TOPIC2, P1);
  private static final int BROKER_ID_0 = 0;
  private static final int BROKER_ID_1 = 1;
  private static final double DELTA = 0.001;
  private final Time _time = new MockTime(0, 100L, TimeUnit.NANOSECONDS.convert(100L, TimeUnit.MILLISECONDS));

  @Test
  public void testBasic() {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor();
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    metrics.forEach(processor::addMetric);

    MetricSampler.Samples samples =
        processor.process(getCluster(), Arrays.asList(T1P0, T1P1, T2P0, T2P1), MetricSampler.SamplingMode.ALL);

    assertEquals(4, samples.partitionMetricSamples().size());
    assertEquals(2, samples.brokerMetricSamples().size());

    for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
      if (sample.entity().tp().equals(T1P0)) {
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, 1.27610208, 20.0, 80.0, 100.0);
      } else if (sample.entity().tp().equals(T1P1)) {
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, 18.5758513, 500.0, 500.0, 300.0);
      } else if (sample.entity().tp().equals(T2P0)) {
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, 20.0116009, 400.0, 650.0, 200.0);
      } else if (sample.entity().tp().equals(T2P1)) {
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, 20.0116009, 400.0, 650.0, 500.0);
      } else {
        fail("Should never have partition " + sample.entity().tp());
      }
    }

    for (BrokerMetricSample sample : samples.brokerMetricSamples()) {
      if (sample.metricValue(CPU_USAGE) == 50.0) {
        assertEquals(500.0, sample.metricValue(REPLICATION_BYTES_IN_RATE), DELTA);
      } else if (sample.metricValue(CPU_USAGE) == 30.0) {
        assertEquals(820.0, sample.metricValue(REPLICATION_BYTES_IN_RATE), DELTA);
      } else {
        fail("Should never have broker cpu util " + sample.metricValue(CPU_USAGE));
      }
    }

    assertTrue(!samples.partitionMetricSamples().isEmpty());
  }

  @Test
  public void testMissingBrokerCpuUtilization() {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor();
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    for (CruiseControlMetric metric : metrics) {
      if (metric.rawMetricType() == RawMetricType.BROKER_CPU_UTIL && metric.brokerId() == BROKER_ID_0) {
        // Do nothing and skip the metric.
      } else {
        processor.addMetric(metric);
      }
    }
    MetricSampler.Samples samples =
        processor.process(getCluster(), Arrays.asList(T1P0, T1P1, T2P0, T2P1), MetricSampler.SamplingMode.ALL);
    assertEquals("Should have ignored partitions on broker 0", 1, samples.partitionMetricSamples().size());
    assertEquals("Should have ignored broker 0", 1, samples.brokerMetricSamples().size());
  }

  @Test
  public void testMissingOtherBrokerMetrics() {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor();
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    for (CruiseControlMetric metric : metrics) {
      if (metric.rawMetricType() == RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX && metric.brokerId() == BROKER_ID_0) {
        // Do nothing and skip the metric.
      } else {
        processor.addMetric(metric);
      }
    }
    MetricSampler.Samples samples =
        processor.process(getCluster(), Arrays.asList(T1P0, T1P1, T2P0, T2P1), MetricSampler.SamplingMode.ALL);
    assertEquals("Should have all 4 partition metrics.", 4, samples.partitionMetricSamples().size());
    assertEquals("Should have ignored broker 0", 1, samples.brokerMetricSamples().size());
  }

  @Test
  public void testMissingPartitionSizeMetric() {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor();
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    for (CruiseControlMetric metric : metrics) {
      boolean shouldAdd = true;
      if (metric.rawMetricType() == RawMetricType.PARTITION_SIZE) {
        PartitionMetric pm = (PartitionMetric) metric;
        if (pm.topic().equals(TOPIC1) && pm.partition() == P0) {
          shouldAdd = false;
        }
      }
      if (shouldAdd) {
        processor.addMetric(metric);
      }
    }
    MetricSampler.Samples samples =
        processor.process(getCluster(), Arrays.asList(T1P0, T1P1, T2P0, T2P1), MetricSampler.SamplingMode.ALL);
    assertEquals("Should have ignored partition " + T1P0, 3, samples.partitionMetricSamples().size());
    assertEquals("Should have reported both brokers", 2, samples.brokerMetricSamples().size());
  }

  @Test
  public void testMissingTopicBytesInMetric() {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor();
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    Set<RawMetricType> metricTypeToExclude = new HashSet<>(Arrays.asList(TOPIC_BYTES_IN,
                                                                         TOPIC_BYTES_OUT,
                                                                         TOPIC_REPLICATION_BYTES_IN,
                                                                         TOPIC_REPLICATION_BYTES_OUT));
    for (CruiseControlMetric metric : metrics) {
      if (metricTypeToExclude.contains(metric.rawMetricType())) {
        TopicMetric tm = (TopicMetric) metric;
        if (tm.brokerId() == BROKER_ID_0 && tm.topic().equals(TOPIC1)) {
          continue;
        }
      }
      processor.addMetric(metric);
    }

    MetricSampler.Samples samples =
        processor.process(getCluster(), Arrays.asList(T1P0, T1P1, T2P0, T2P1), MetricSampler.SamplingMode.ALL);

    assertEquals(4, samples.partitionMetricSamples().size());
    assertEquals(2, samples.brokerMetricSamples().size());

    for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
      if (sample.entity().tp().equals(T1P0)) {
        // T1P0 should not have any IO or CPU usage.
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, 0.0, 0.0, 0.0, 100.0);
      }
    }
  }

  /**
   * T1P0(B0): NW_IN = 20 Bps, NW_OUT = 80 Bps, size = 100 MB
   * T1P1(B1): NW_IN = 500 Bps, NW_OUT = 500 Bps, size = 300 MB
   * T2P0(B0): NW_IN = 300 Bps, NW_OUT = 300 Bps, size = 200 MB
   * T2P1(B0): NW_IN = 500 Bps, NW_OUT = 1000 Bps, size = 500 MB
   * B0: CPU = 50%
   * B1: CPU = 30%
   */
  private Set<CruiseControlMetric> getCruiseControlMetrics() {
    Set<CruiseControlMetric> metrics = new HashSet<>();

    int i = 0;
    for (RawMetricType rawMetricType : RawMetricType.brokerMetricTypes()) {
      switch (rawMetricType) {
        case ALL_TOPIC_BYTES_IN:
          metrics.add(new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_IN, _time.milliseconds(), BROKER_ID_0, 820.0 * BYTES_IN_KB));
          metrics.add(new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_IN, _time.milliseconds(), BROKER_ID_1, 500.0 * BYTES_IN_KB));
          break;
        case ALL_TOPIC_BYTES_OUT:
          metrics.add(new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, 1380.0 * BYTES_IN_KB));
          metrics.add(new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_1, 500.0 * BYTES_IN_KB));
          break;
        case BROKER_CPU_UTIL:
          metrics.add(new BrokerMetric(RawMetricType.BROKER_CPU_UTIL, _time.milliseconds(), BROKER_ID_0, 50.0));
          metrics.add(new BrokerMetric(RawMetricType.BROKER_CPU_UTIL, _time.milliseconds(), BROKER_ID_1, 30.0));
          break;
        default:
          metrics.add(new BrokerMetric(rawMetricType, _time.milliseconds(), BROKER_ID_0, i++ * BYTES_IN_MB));
          metrics.add(new BrokerMetric(rawMetricType, _time.milliseconds(), BROKER_ID_1, i++ * BYTES_IN_MB));
          break;
      }
    }

    for (RawMetricType rawMetricType : RawMetricType.topicMetricTypes()) {
      switch (rawMetricType) {
        case TOPIC_BYTES_IN:
          metrics.add(new TopicMetric(TOPIC_BYTES_IN, _time.milliseconds() + 1, BROKER_ID_0, TOPIC1, 20.0 * BYTES_IN_KB));
          metrics.add(new TopicMetric(TOPIC_BYTES_IN, _time.milliseconds() + 2, BROKER_ID_1, TOPIC1, 500.0 * BYTES_IN_KB));
          metrics.add(new TopicMetric(TOPIC_BYTES_IN, _time.milliseconds(), BROKER_ID_0, TOPIC2, 800.0 * BYTES_IN_KB));
          break;
        case TOPIC_BYTES_OUT:
          metrics.add(new TopicMetric(RawMetricType.TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, TOPIC1, 80.0 * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_1, TOPIC1, 500.0 * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, TOPIC2, 1300.0 * BYTES_IN_KB));
          break;
        case TOPIC_REPLICATION_BYTES_IN:
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_IN, _time.milliseconds(), BROKER_ID_1, TOPIC1, 20.0 * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_IN, _time.milliseconds(), BROKER_ID_0, TOPIC1, 500.0 * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_IN, _time.milliseconds(), BROKER_ID_1, TOPIC2, 800.0 * BYTES_IN_KB));
          break;
        case TOPIC_REPLICATION_BYTES_OUT:
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, TOPIC1, 20.0 * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, _time.milliseconds(), BROKER_ID_1, TOPIC1, 500.0 * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, TOPIC2, 800.0 * BYTES_IN_KB));
          break;
        default:
          metrics.add(new TopicMetric(rawMetricType, _time.milliseconds(),  BROKER_ID_0, TOPIC1, i * BYTES_IN_MB));
          metrics.add(new TopicMetric(rawMetricType, _time.milliseconds(),  BROKER_ID_1, TOPIC1, i * BYTES_IN_MB));
          metrics.add(new TopicMetric(rawMetricType, _time.milliseconds(),  BROKER_ID_0, TOPIC2, i * BYTES_IN_MB));
          metrics.add(new TopicMetric(rawMetricType, _time.milliseconds(),  BROKER_ID_1, TOPIC2, i * BYTES_IN_MB));
          break;
      }
    }

    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_0, TOPIC1, P0, 100 * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_0, TOPIC1, P1, 300 * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_0, TOPIC2, P0, 200 * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_0, TOPIC2, P1, 500 * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_1, TOPIC1, P0, 100 * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_1, TOPIC1, P1, 300 * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_1, TOPIC2, P0, 200 * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_1, TOPIC2, P1, 500 * BYTES_IN_MB));
    return metrics;
  }

  private void validatePartitionMetricSample(PartitionMetricSample sample, long time, double cpu, double bytesIn, double bytesOut,
                                             double disk) {
    assertEquals(time, sample.sampleTime());
    assertEquals(cpu, sample.metricValue(KafkaMetricDef.commonMetricDefId(CPU_USAGE)), DELTA);
    assertEquals(bytesIn, sample.metricValue(KafkaMetricDef.commonMetricDefId(LEADER_BYTES_IN)), DELTA);
    assertEquals(bytesOut, sample.metricValue(KafkaMetricDef.commonMetricDefId(LEADER_BYTES_OUT)), DELTA);
    assertEquals(disk, sample.metricValue(KafkaMetricDef.commonMetricDefId(DISK_USAGE)), DELTA);
  }

  private Cluster getCluster() {
    Node node0 = new Node(BROKER_ID_0, "localhost", 100, "rack0");
    Node node1 = new Node(BROKER_ID_1, "localhost", 100, "rack1");
    Node[] nodes = {node0, node1};
    Set<Node> allNodes = new HashSet<>();
    allNodes.add(node0);
    allNodes.add(node1);
    Set<PartitionInfo> parts = new HashSet<>();
    parts.add(new PartitionInfo(TOPIC1, P0, node0, nodes, nodes));
    parts.add(new PartitionInfo(TOPIC1, P1, node1, nodes, nodes));
    parts.add(new PartitionInfo(TOPIC2, P0, node0, nodes, nodes));
    parts.add(new PartitionInfo(TOPIC2, P1, node0, nodes, nodes));
    return new Cluster("testCluster", allNodes, parts, Collections.emptySet(), Collections.emptySet());
  }
}
