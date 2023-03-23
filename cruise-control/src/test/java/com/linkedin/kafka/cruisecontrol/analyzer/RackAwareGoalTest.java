/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IgnorePrefixRackIdMapper;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class RackAwareGoalTest {
  private final Goal _goalToTest;

  public RackAwareGoalTest(Goal goalToTest) {
    _goalToTest = goalToTest;
  }

  /**
   * This is to provide parameterization.
   *
   * @return Objects to pass to test constructor
   */
  @Parameterized.Parameters
  public static Collection<Object[]> goalNames() {
    return Arrays.asList(new Object[]{new RackAwareDistributionGoal()},
                         new Object[]{new RackAwareGoal()});
  }

  /**
   * Test if the RackAwareGoals honors the mapper class.
   * <p>
   * In this test case, with the mapper, broker 0 & 1 should be considered on the same rack 0, because "A::" and "B::" will be striped off by
   * the mapper.
   * <p>
   * If the mapper is honored, the optimization would consider replicas on broker 0 & 1 are on the same rack, and recommend to move
   * one of it to broker 2
   */
  @Test
  public void testRackIdMapper() throws Exception {
    /////////////
    // Arrange //
    /////////////
    // Declare cluster layout
    final Map<Integer, String> brokerToRack = Map.of(0, "A::0",
                                                     1, "B::0",
                                                     2, "C::1");

    final ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY);
    brokerToRack.values().stream().distinct().forEach(cluster::createRack);
    brokerToRack.forEach((broker, rack) -> cluster.createBroker(
        rack, Integer.toString(broker), broker,
        commonBrokerCapacityInfo,
        commonBrokerCapacityInfo.diskCapacityByLogDir() != null
    ));

    // Create topic partition.
    TopicPartition tp = new TopicPartition("topic", 0);

    // Create replicas for topic one broker 0, 1, 2.
    cluster.createReplica(brokerToRack.get(0), 0, tp, 0, true);
    cluster.createReplica(brokerToRack.get(1), 1, tp, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad(brokerToRack.get(0), 0, tp, getAggregatedMetricValues(40.0, 100.0, 130.0, 75.0), windows);
    cluster.setReplicaLoad(brokerToRack.get(1), 1, tp, getAggregatedMetricValues(5.0, 100.0, 0.0, 75.0), windows);

    // Set mapper class
    final Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.RACK_AWARE_GOAL_RACK_ID_MAPPER_CLASS_CONFIG, IgnorePrefixRackIdMapper.class.getCanonicalName());

    /////////////
    //   Act   //
    /////////////
    final KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    List<Goal> goalsByPriority = Collections.singletonList(_goalToTest);
    _goalToTest.configure(kafkaCruiseControlConfig.mergedConfigValues());
    GoalOptimizer goalOptimizer = new GoalOptimizer(kafkaCruiseControlConfig,
                                                    null,
                                                    new SystemTime(),
                                                    new MetricRegistry(),
                                                    EasyMock.mock(Executor.class),
                                                    EasyMock.mock(AdminClient.class));
    final Set<ExecutionProposal> proposals =
        goalOptimizer.optimizations(cluster, goalsByPriority, new OperationProgress()).goalProposals();

    ////////////
    // Assert //
    ////////////
    assertEquals(1, proposals.size());
    final ExecutionProposal proposal = proposals.iterator().next();
    // Should recommend to move a replica to broker 2, either from broker 0 or 1
    assertEquals(tp, proposal.topicPartition());
    assertFalse(Collections.disjoint(proposal.replicasToRemove(), Set.of(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1))));
    assertEquals(Set.of(new ReplicaPlacementInfo(2)), proposal.replicasToAdd());
  }

  /**
   * In this test case, mapper is not set, so broker 0 & 1 should be considered on different rack and no recommendation should be made.
   */
  @Test
  public void testWithoutRackIdMapper() throws Exception {
    /////////////
    // Arrange //
    /////////////
    // Declare cluster layout
    final Map<Integer, String> brokerToRack = Map.of(0, "A::0",
                                                     1, "B::0",
                                                     2, "C::1");

    final ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY);
    brokerToRack.values().stream().distinct().forEach(cluster::createRack);

    brokerToRack.forEach((broker, rack) -> cluster.createBroker(
        rack, Integer.toString(broker), broker,
        commonBrokerCapacityInfo,
        commonBrokerCapacityInfo.diskCapacityByLogDir() != null
    ));

    // Create topic partition.
    TopicPartition tp = new TopicPartition("topic", 0);

    // Create replicas for topic one broker 0, 1, 2.
    cluster.createReplica(brokerToRack.get(0), 0, tp, 0, true);
    cluster.createReplica(brokerToRack.get(1), 1, tp, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad(brokerToRack.get(0), 0, tp, getAggregatedMetricValues(40.0, 100.0, 130.0, 75.0), windows);
    cluster.setReplicaLoad(brokerToRack.get(1), 1, tp, getAggregatedMetricValues(5.0, 100.0, 0.0, 75.0), windows);

    // Use default config, without mapper class
    final Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();

    /////////////
    //   Act   //
    /////////////
    final KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    List<Goal> goalsByPriority = Collections.singletonList(_goalToTest);
    _goalToTest.configure(kafkaCruiseControlConfig.mergedConfigValues());
    GoalOptimizer goalOptimizer = new GoalOptimizer(kafkaCruiseControlConfig,
                                                    null,
                                                    new SystemTime(),
                                                    new MetricRegistry(),
                                                    EasyMock.mock(Executor.class),
                                                    EasyMock.mock(AdminClient.class));
    final Set<ExecutionProposal> proposals =
        goalOptimizer.optimizations(cluster, goalsByPriority, new OperationProgress()).goalProposals();

    ////////////
    // Assert //
    ////////////
    assertTrue(proposals.isEmpty());
  }
}
