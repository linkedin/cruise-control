/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache.MinIsrWithTime;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ConcurrencyAdjusterTest {
  private static final double MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT = 100.0;
  private static final int NUM_BROKERS = 4;
  private static final Random RANDOM = new Random(0xDEADBEEF);
  private static final int MOCK_ADDITIVE_INCREASE_INTER_BROKER_REPLICA = 2;
  private static final int MOCK_ADDITIVE_INCREASE_LEADERSHIP = 50;
  private static final int MOCK_MD_INTER_BROKER_REPLICA = 2;
  private static final int MOCK_MD_LEADERSHIP = 3;
  private static final int MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER = 12;
  private static final int MOCK_MAX_LEADERSHIP_MOVEMENTS = 1000;
  private static final int MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER = 1;
  private static final int MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG = 50;
  private static final long MOCK_TIME_MS = 100L;
  private static final String TOPIC1 = "topic1";
  private static final TopicPartition TP1 = new TopicPartition(TOPIC1, 0);

  /**
   * Setup the test.
   */
  @BeforeClass
  public static void setup() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    ExecutionUtils.init(config);
  }

  private static Map<Short, Double> populateMetricValues(int numOverLimitMetrics) {
    Map<Short, Double> metricValueById = new HashMap<>();
    int remainingOverLimitMetrics = numOverLimitMetrics;
    for (String name : ExecutionUtils.CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.keySet()) {
      // Generate a number in [1.0, MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT + 1.0]
      double diff = 1.0 + (MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT * RANDOM.nextDouble());
      if (--remainingOverLimitMetrics < 0) {
        diff *= -1.0;
      }
      metricValueById.put(KafkaMetricDef.brokerMetricDef().metricInfo(name).id(), MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT + diff);
    }

    return metricValueById;
  }

  /**
   * Create current load snapshot for brokers.
   * @param metricValueByIdPerBroker A set of metrics with corresponding load value per broker.
   * @return The load for the brokers.
   */
  public static Map<BrokerEntity, ValuesAndExtrapolations> createCurrentMetrics(List<Map<Short, Double>> metricValueByIdPerBroker) {
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>();
    for (int i = 0; i < metricValueByIdPerBroker.size(); i++) {
      Map<Short, MetricValues> valuesByMetricId = new HashMap<>();
      for (Map.Entry<Short, Double> entry : metricValueByIdPerBroker.get(i).entrySet()) {
        MetricValues currentMetricValues = new MetricValues(1);
        double[] values = new double[] {entry.getValue()};
        currentMetricValues.add(values);
        valuesByMetricId.put(entry.getKey(), currentMetricValues);
      }
      AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
      ValuesAndExtrapolations currentValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, null);
      List<Long> windows = new ArrayList<>(1);
      windows.add(1L);
      currentValuesAndExtrapolations.setWindows(windows);
      currentMetrics.put(new BrokerEntity(ExecutorTest.class.getSimpleName(), i), currentValuesAndExtrapolations);
    }

    return currentMetrics;
  }

  /**
   * A cluster that contains a single partition {@link #TP1} with two replicas on Nodes 0 and 1.
   * Staring from node-0, given number of replicas are in-sync.
   * Out of sync replicas are offline if isOutOfSyncOffline is {@code true}, they are online otherwise.
   *
   * @param numInSync Number of in-sync replicas.
   * @param isOutOfSyncOffline {@code true} if out of sync replicas are offline, {@code false} otherwise.
   * @return A cluster that contains a single partition with two replicas, containing given number of in-sync replicas.
   */
  private static Cluster getClusterWithOutOfSyncPartition(int numInSync, boolean isOutOfSyncOffline) {
    if (numInSync > 2 || numInSync < 0) {
      throw new IllegalArgumentException(String.format("numInSync must be in [0,2] (Given: %d).", numInSync));
    }

    Node node0 = new Node(0, "host0", 100);
    Node node1 = new Node(1, "host1", 100);
    Node[] replicas = new Node[2];
    replicas[0] = node0;
    replicas[1] = node1;
    Node[] inSyncReplicas = new Node[numInSync];
    if (numInSync > 0) {
      inSyncReplicas[0] = node0;
      if (numInSync > 1) {
        inSyncReplicas[1] = node1;
      }
    }
    PartitionInfo partitionInfo;
    if (!isOutOfSyncOffline) {
      partitionInfo = new PartitionInfo(TP1.topic(), TP1.partition(), node0, replicas, inSyncReplicas);
    } else {
      Node[] offlineReplicas = new Node[2 - numInSync];
      if (numInSync == 1) {
        offlineReplicas[0] = node1;
      } else if (numInSync == 0) {
        offlineReplicas[0] = node0;
        offlineReplicas[1] = node1;
      }

      partitionInfo = new PartitionInfo(TP1.topic(), TP1.partition(), node0, replicas, inSyncReplicas, offlineReplicas);
    }
    return new Cluster("id", Arrays.asList(node0, node1), Collections.singleton(partitionInfo),
                       Collections.emptySet(), Collections.emptySet());

  }

  @Test
  public void testRecommendedMinIsrBasedConcurrency() {
    // Cluster with an online out of sync partition.
    Cluster cluster = getClusterWithOutOfSyncPartition(1, false);

    // 1. Verify a recommended decrease in concurrency for different concurrency types due to AtMinISR partitions without offline replicas.
    // Cache with a single entry that makes the TP1 in cluster AtMinISR.
    Map<String, MinIsrWithTime> minIsrWithTimeByTopic = Collections.singletonMap(TOPIC1, new MinIsrWithTime((short) 1, MOCK_TIME_MS));

    // 1.1. Inter-broker replica reassignment (non-capped)
    Integer recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                           minIsrWithTimeByTopic,
                                                                           MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                                                                           ConcurrencyType.INTER_BROKER_REPLICA);
    assertEquals(MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER / MOCK_MD_INTER_BROKER_REPLICA, recommendedConcurrency.intValue());

    // 1.2. Leadership reassignment (non-capped)
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   minIsrWithTimeByTopic,
                                                                   MOCK_MAX_LEADERSHIP_MOVEMENTS,
                                                                   ConcurrencyType.LEADERSHIP);
    assertEquals(MOCK_MAX_LEADERSHIP_MOVEMENTS / MOCK_MD_LEADERSHIP, recommendedConcurrency.intValue());

    // 1.3. Inter-broker replica reassignment (capped)
    int currentMovementConcurrency = MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER * MOCK_MD_INTER_BROKER_REPLICA;
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   minIsrWithTimeByTopic,
                                                                   currentMovementConcurrency,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertEquals(MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER, recommendedConcurrency.intValue());

    // 1.4. Leadership reassignment (capped)
    currentMovementConcurrency = (MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG * MOCK_MD_LEADERSHIP) - 1;
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   minIsrWithTimeByTopic,
                                                                   currentMovementConcurrency,
                                                                   ConcurrencyType.LEADERSHIP);
    assertEquals(MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG, recommendedConcurrency.intValue());

    // 2. Verify a recommended cancellation of the execution (i.e. concurrency types is irrelevant) due to UnderMinISR partitions without
    // offline replicas.
    // Cache with a single entry that makes the TP1 in cluster UnderMinISR.
    minIsrWithTimeByTopic = Collections.singletonMap(TOPIC1, new MinIsrWithTime((short) 2, MOCK_TIME_MS));
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   minIsrWithTimeByTopic,
                                                                   MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertEquals(ExecutionUtils.CANCEL_THE_EXECUTION, recommendedConcurrency.intValue());

    // 3. Verify that if the minISR value for topics containing (At/Under)MinISR partitions in the given Kafka cluster is missing from the
    // given cache, then no change in concurrency is recommended.
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   Collections.emptyMap(),
                                                                   MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertNull(recommendedConcurrency);

    // 4. Verify no change in concurrency due to lack of (At/Under)MinISR partitions (i.e. concurrency types is irrelevant)
    // Cluster with an all in-sync partition.
    cluster = getClusterWithOutOfSyncPartition(2, false);
    // Cache with a single entry that makes the TP1 in cluster not (At/Under)MinISR.
    minIsrWithTimeByTopic = Collections.singletonMap(TOPIC1, new MinIsrWithTime((short) 1, MOCK_TIME_MS));

    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   minIsrWithTimeByTopic,
                                                                   MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertNull(recommendedConcurrency);

    // 5. Verify no change in concurrency due to (At/Under)MinISR partitions with an offline replica (i.e. concurrency types is irrelevant)
    // Cluster with an offline out of sync partition.
    cluster = getClusterWithOutOfSyncPartition(1, true);
    // 5.1. Recommendation with AtMinISR partition containing an offline replica.
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   minIsrWithTimeByTopic,
                                                                   MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertNull(recommendedConcurrency);

    // 5.2. Recommendation with UnderMinISR partition containing an offline replica.
    // Cache with a single entry that makes the TP1 in cluster UnderMinISR.
    minIsrWithTimeByTopic = Collections.singletonMap(TOPIC1, new MinIsrWithTime((short) 2, MOCK_TIME_MS));
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   minIsrWithTimeByTopic,
                                                                   MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertNull(recommendedConcurrency);
  }

  @Test
  public void testRecommendedConcurrency() {
    // 1. Verify a recommended increase in concurrency for different concurrency types.
    List<Map<Short, Double>> metricValueByIdPerBroker = new ArrayList<>(NUM_BROKERS);
    for (int i = 0; i < NUM_BROKERS; i++) {
      metricValueByIdPerBroker.add(populateMetricValues(0));
    }
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = createCurrentMetrics(metricValueByIdPerBroker);

    // 1.1. Inter-broker replica reassignment (non-capped)
    int currentMovementConcurrency = MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER - MOCK_ADDITIVE_INCREASE_INTER_BROKER_REPLICA - 1;
    Integer recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                           currentMovementConcurrency,
                                                                           ConcurrencyType.INTER_BROKER_REPLICA);
    assertEquals(currentMovementConcurrency + MOCK_ADDITIVE_INCREASE_INTER_BROKER_REPLICA, recommendedConcurrency.intValue());

    // 1.2. Leadership reassignment (non-capped)
    currentMovementConcurrency = MOCK_MAX_LEADERSHIP_MOVEMENTS - MOCK_ADDITIVE_INCREASE_LEADERSHIP - 1;
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   currentMovementConcurrency,
                                                                   ConcurrencyType.LEADERSHIP);
    assertEquals(currentMovementConcurrency + MOCK_ADDITIVE_INCREASE_LEADERSHIP, recommendedConcurrency.intValue());

    // 1.3. Inter-broker replica reassignment (capped)
    currentMovementConcurrency = MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER - MOCK_ADDITIVE_INCREASE_INTER_BROKER_REPLICA + 1;
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   currentMovementConcurrency,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertEquals(MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER, recommendedConcurrency.intValue());

    // 1.4. Leadership reassignment (capped)
    currentMovementConcurrency = MOCK_MAX_LEADERSHIP_MOVEMENTS - MOCK_ADDITIVE_INCREASE_LEADERSHIP + 1;
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   currentMovementConcurrency,
                                                                   ConcurrencyType.LEADERSHIP);
    assertEquals(MOCK_MAX_LEADERSHIP_MOVEMENTS, recommendedConcurrency.intValue());

    // 2. Verify no change in concurrency due to hitting max limit for different concurrency types.
    // 2.1. Inter-broker replica reassignment
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertNull(recommendedConcurrency);

    // 2.2. Leadership reassignment
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   MOCK_MAX_LEADERSHIP_MOVEMENTS,
                                                                   ConcurrencyType.LEADERSHIP);
    assertNull(recommendedConcurrency);

    // 3. Verify a recommended decrease in concurrency for different concurrency types.
    metricValueByIdPerBroker.add(populateMetricValues(1));
    currentMetrics = createCurrentMetrics(metricValueByIdPerBroker);
    // 3.1. Inter-broker replica reassignment (non-capped)
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertEquals(MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER / MOCK_MD_INTER_BROKER_REPLICA, recommendedConcurrency.intValue());

    // 3.2. Leadership reassignment (non-capped)
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   MOCK_MAX_LEADERSHIP_MOVEMENTS,
                                                                   ConcurrencyType.LEADERSHIP);
    assertEquals(MOCK_MAX_LEADERSHIP_MOVEMENTS / MOCK_MD_LEADERSHIP, recommendedConcurrency.intValue());

    // 3.3. Inter-broker replica reassignment (capped)
    currentMovementConcurrency = MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER * MOCK_MD_INTER_BROKER_REPLICA;
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   currentMovementConcurrency,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertEquals(MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER, recommendedConcurrency.intValue());

    // 3.4. Leadership reassignment (capped)
    currentMovementConcurrency = (MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG * MOCK_MD_LEADERSHIP) - 1;
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   currentMovementConcurrency,
                                                                   ConcurrencyType.LEADERSHIP);
    assertEquals(MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG, recommendedConcurrency.intValue());

    // 4. Verify no change in concurrency due to hitting lower limit.
    // 4.1. Inter-broker replica reassignment
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER,
                                                                   ConcurrencyType.INTER_BROKER_REPLICA);
    assertNull(recommendedConcurrency);

    // 4.2. Leadership reassignment
    recommendedConcurrency = ExecutionUtils.recommendedConcurrency(currentMetrics,
                                                                   MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG,
                                                                   ConcurrencyType.LEADERSHIP);
    assertNull(recommendedConcurrency);
  }

  @Test
  public void testWithinConcurrencyAdjusterLimit() {
    // Verify within the limit by adding brokers under the limit.
    List<Map<Short, Double>> metricValueByIdPerBroker = new ArrayList<>(NUM_BROKERS);
    for (int i = 0; i < NUM_BROKERS; i++) {
      metricValueByIdPerBroker.add(populateMetricValues(0));
    }
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = createCurrentMetrics(metricValueByIdPerBroker);
    assertTrue(ExecutionUtils.withinConcurrencyAdjusterLimit(currentMetrics));

    // Verify over the limit by adding a broker with just one metric over the limit.
    metricValueByIdPerBroker.add(populateMetricValues(1));
    currentMetrics = createCurrentMetrics(metricValueByIdPerBroker);
    assertFalse(ExecutionUtils.withinConcurrencyAdjusterLimit(currentMetrics));

    // Verify over the limit by having a broker with no metrics.
    metricValueByIdPerBroker.remove(NUM_BROKERS);
    currentMetrics = createCurrentMetrics(metricValueByIdPerBroker);
    currentMetrics.put(new BrokerEntity(ExecutorTest.class.getSimpleName(), 42), null);
    assertFalse(ExecutionUtils.withinConcurrencyAdjusterLimit(currentMetrics));
  }

  private static Properties getExecutorProperties() {
    Properties props = new Properties();
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
    props.setProperty(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2121");
    props.setProperty(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, "10");
    props.setProperty(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "400");
    props.setProperty(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "200");
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA_CONFIG,
                      Integer.toString(MOCK_ADDITIVE_INCREASE_INTER_BROKER_REPLICA));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_CONFIG,
                      Integer.toString(MOCK_ADDITIVE_INCREASE_LEADERSHIP));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA_CONFIG,
                      Integer.toString(MOCK_MD_INTER_BROKER_REPLICA));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_CONFIG,
                      Integer.toString(MOCK_MD_LEADERSHIP));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                      Integer.toString(MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG,
                      Integer.toString(MOCK_MAX_LEADERSHIP_MOVEMENTS));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                      Integer.toString(MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG,
                      Integer.toString(MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG));

    return props;
  }
}
