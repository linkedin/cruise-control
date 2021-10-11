/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSamplerOptions;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.METADATA_EXPIRY_MS;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.METADATA_REFRESH_BACKOFF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 * The unit test for metric fetcher manager.
 */
public class LoadMonitorTaskRunnerTest extends CCKafkaIntegrationTestHarness {
  private static final long WINDOW_MS = TimeUnit.SECONDS.toMillis(10);
  private static final int NUM_WINDOWS = 5;
  private static final int NUM_TOPICS = 100;
  private static final int NUM_PARTITIONS = 4;
  private static final long SAMPLING_INTERVAL_MS = TimeUnit.SECONDS.toMillis(100);
  private static final MetricDef METRIC_DEF = KafkaMetricDef.commonMetricDef();
  private static final String TOPIC_PREFIX = "topic-";
  // Using autoTick = 1
  private static final Time TIME = new MockTime(1L);

  /**
   * Setup the test.
   */
  @Before
  public void setUp() {
    super.setUp();
    Properties adminProps = new Properties();
    adminProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    AdminClient adminClient = AdminClient.create(adminProps);
    Set<NewTopic> newTopics = new HashSet<>();
    for (int i = 0; i < NUM_TOPICS; i++) {
      newTopics.add(new NewTopic(TOPIC_PREFIX + i, NUM_PARTITIONS, (short) 1));
    }
    CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);

    AtomicInteger adminFailed = new AtomicInteger(0);
    createTopicsResult.all().whenComplete((v, e) -> {
      if (e != null) {
        adminFailed.incrementAndGet();
      }
    });
    assertEquals(0, adminFailed.get());
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Override
  public int clusterSize() {
    return 1;
  }

  @Test
  public void testSimpleFetch() throws InterruptedException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = new Metadata(METADATA_REFRESH_BACKOFF,
                                     METADATA_EXPIRY_MS,
                                     new LogContext(),
                                     new ClusterResourceListeners());
    MetadataClient metadataClient = new MetadataClient(config, metadata, -1L, TIME);
    assertNotNull(metadataClient.cluster().clusterResource().clusterId());
    MockPartitionMetricSampleAggregator mockPartitionMetricSampleAggregator =
        new MockPartitionMetricSampleAggregator(config, metadata);
    KafkaBrokerMetricSampleAggregator mockBrokerMetricSampleAggregator =
        EasyMock.mock(KafkaBrokerMetricSampleAggregator.class);
    MetricRegistry dropwizardMetricRegistry = new MetricRegistry();
    MetricSampler sampler = new MockSampler(0);
    MetricFetcherManager fetcherManager =
        new MetricFetcherManager(config, mockPartitionMetricSampleAggregator, mockBrokerMetricSampleAggregator,
                                 metadataClient, METRIC_DEF, TIME, dropwizardMetricRegistry, null, sampler);
    LoadMonitorTaskRunner loadMonitorTaskRunner =
        new LoadMonitorTaskRunner(config, fetcherManager, mockPartitionMetricSampleAggregator,
                                  mockBrokerMetricSampleAggregator, metadataClient, TIME);
    while (metadata.fetch().topics().size() < NUM_TOPICS) {
      Thread.sleep(10);
      metadataClient.refreshMetadata();
    }
    loadMonitorTaskRunner.start(true);

    Set<TopicPartition> partitionsToSample = new HashSet<>();
    for (int i = 0; i < NUM_TOPICS; i++) {
      for (int j = 0; j < NUM_PARTITIONS; j++) {
        partitionsToSample.add(new TopicPartition(TOPIC_PREFIX + i, j));
      }
    }

    long startMs = System.currentTimeMillis();
    BlockingQueue<PartitionMetricSample> sampleQueue = mockPartitionMetricSampleAggregator.metricSampleQueue();
    while (!partitionsToSample.isEmpty() && System.currentTimeMillis() < startMs + 10000) {
      PartitionMetricSample sample = sampleQueue.poll();
      if (sample != null) {
        assertTrue("The topic partition should have been sampled and sampled only once.",
            partitionsToSample.contains(sample.entity().tp()));
        partitionsToSample.remove(sample.entity().tp());
      }
    }
    assertTrue("Did not see sample for partitions " + Arrays.toString(partitionsToSample.toArray()),
        partitionsToSample.isEmpty());
    fetcherManager.shutdown();
    assertTrue(sampleQueue.isEmpty());
  }

  @Test
  public void testSamplingError() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = new Metadata(METADATA_REFRESH_BACKOFF,
                                     METADATA_EXPIRY_MS,
                                     new LogContext(),
                                     new ClusterResourceListeners());
    MetadataClient metadataClient = new MetadataClient(config, metadata, -1L, TIME);
    assertNotNull(metadataClient.cluster().clusterResource().clusterId());
    MockPartitionMetricSampleAggregator mockMetricSampleAggregator =
        new MockPartitionMetricSampleAggregator(config, metadata);
    KafkaBrokerMetricSampleAggregator mockBrokerMetricSampleAggregator =
        EasyMock.mock(KafkaBrokerMetricSampleAggregator.class);
    MetricRegistry dropwizardMetricRegistry = new MetricRegistry();
    MetricSampler sampler = new MockSampler(0);
    MetricFetcherManager fetcherManager =
        new MetricFetcherManager(config, mockMetricSampleAggregator, mockBrokerMetricSampleAggregator, metadataClient,
                                 METRIC_DEF, TIME, dropwizardMetricRegistry, null, sampler);
    LoadMonitorTaskRunner loadMonitorTaskRunner =
        new LoadMonitorTaskRunner(config, fetcherManager, mockMetricSampleAggregator, mockBrokerMetricSampleAggregator,
                                  metadataClient, TIME);
    while (metadata.fetch().topics().size() < 100) {
      metadataClient.refreshMetadata();
    }
    loadMonitorTaskRunner.start(true);

    int numSamples = 0;
    long startMs = System.currentTimeMillis();
    BlockingQueue<PartitionMetricSample> sampleQueue = mockMetricSampleAggregator.metricSampleQueue();
    while (numSamples < (NUM_PARTITIONS * NUM_TOPICS) * 10 && System.currentTimeMillis() < startMs + 10000) {
      PartitionMetricSample sample = sampleQueue.poll();
      if (sample != null) {
        numSamples++;
      }
    }
    int expectedNumSamples = NUM_TOPICS * NUM_PARTITIONS;
    assertEquals("Only see " + numSamples + " samples. Expecting " + expectedNumSamples + " samples",
        expectedNumSamples, numSamples);
    fetcherManager.shutdown();
  }

  private Properties getLoadMonitorProperties() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, Long.toString(WINDOW_MS));
    props.setProperty(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, Integer.toString(NUM_WINDOWS));
    // The configuration does not matter here, we pass in the fetcher explicitly.
    props.setProperty(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, MockSampler.class.getName());
    props.setProperty(MonitorConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG, Long.toString(SAMPLING_INTERVAL_MS));
    props.setProperty(MonitorConfig.SAMPLE_STORE_CLASS_CONFIG, NoopSampleStore.class.getName());
    props.setProperty(MonitorConfig.SAMPLE_PARTITION_METRIC_STORE_ON_EXECUTION_CLASS_CONFIG, NoopSampleStore.class.getName());
    return props;
  }

  // A simple metric sampler that increment the mock time by 1
  private static class MockSampler implements MetricSampler {
    private int _exceptionsLeft;

    MockSampler(int numExceptions) {
      _exceptionsLeft = numExceptions;
    }

    @Override
    public Samples getSamples(Cluster cluster,
                              Set<TopicPartition> assignedPartitions,
                              long startTime,
                              long endTime,
                              SamplingMode mode,
                              MetricDef metricDef,
                              long timeoutMs) throws SamplingException {
      return getSamples(
          new MetricSamplerOptions(cluster, assignedPartitions, startTime, endTime, mode, metricDef, timeoutMs));
    }

    @Override
    public Samples getSamples(MetricSamplerOptions metricSamplerOptions) throws SamplingException {
      if (_exceptionsLeft > 0) {
        _exceptionsLeft--;
        throw new SamplingException("Error");
      }
      Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
      for (TopicPartition tp : metricSamplerOptions.assignedPartitions()) {
        PartitionMetricSample sample = new PartitionMetricSample(
            metricSamplerOptions.cluster().partition(tp).leader().id(), tp);
        long now = TIME.milliseconds();
        for (Resource resource : Resource.cachedValues()) {
          for (MetricInfo metricInfo : KafkaMetricDef.resourceToMetricInfo(resource)) {
            sample.record(metricInfo, now);
          }
        }
        sample.close(now);
        partitionMetricSamples.add(sample);
      }

      return new Samples(partitionMetricSamples, Collections.emptySet());
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {

    }
  }

  /**
   * A clock that you can manually advance by calling sleep.
   */
  private static class MockTime implements Time {
    private long _nanos;
    private final long _autoTickMs;

    MockTime(long autoTickMs) {
      _nanos = 0;
      _autoTickMs = autoTickMs;
    }

    @Override
    public long milliseconds() {
      this.sleep(_autoTickMs);
      return TimeUnit.MILLISECONDS.convert(this._nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long hiResClockMs() {
      return 0;
    }

    @Override
    public long nanoseconds() {
      this.sleep(_autoTickMs);
      return _nanos;
    }

    @Override
    public void sleep(long ms) {
      this._nanos += TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
    }

    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long timeoutMs) {
      // Noop
    }
  }

  private static class MockPartitionMetricSampleAggregator extends KafkaPartitionMetricSampleAggregator {
    private final BlockingQueue<PartitionMetricSample> _partitionMetricSamples;
    /**
     * Construct the metric sample aggregator.
     * @param config   The load monitor configurations.
     * @param metadata The metadata of the cluster.
     */
    MockPartitionMetricSampleAggregator(KafkaCruiseControlConfig config, Metadata metadata) {
      super(config, metadata);
      _partitionMetricSamples = new ArrayBlockingQueue<>(10000);
    }

    @Override
    public synchronized boolean addSample(PartitionMetricSample sample) {
      _partitionMetricSamples.add(sample);
      return true;
    }

    @Override
    public synchronized boolean addSample(PartitionMetricSample sample, boolean skipLeaderCheck) {
      _partitionMetricSamples.add(sample);
      return true;
    }

    public BlockingQueue<PartitionMetricSample> metricSampleQueue() {
      return _partitionMetricSamples;
    }
  }
}
