/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ResourceDistributionGoalTest {
  private static final Map<Integer, Integer> RACK_BY_BROKER;
  private static final Map<Resource, Double> BROKER_CAPACITY;

  private static final String LARGE_TOPIC = "large-topic";
  private static final String EMPTY_TOPIC = "empty-topic";

  static {
    RACK_BY_BROKER = Map.of(0, 0, 1, 1, 2, 2);
  }
  static {
    BROKER_CAPACITY = Map.of(Resource.DISK, 100.0, Resource.CPU, 100.0, Resource.NW_IN, 100.0, Resource.NW_OUT, 100.0);
  }

  @Test
  public void testActionAcceptanceForEmptyPartitions() throws Exception {
    final var clusterModel = DeterministicCluster.getHomogeneousCluster(RACK_BY_BROKER, BROKER_CAPACITY, null);
    // with only 4 partitions (8 with RF=2), the disk usage goal will be violated
    // brokers 0 and 1 will be at 90% usage, broker 2 will be at 60% usage
    roundRobinPartitionsOnBrokers(clusterModel, LARGE_TOPIC, 4, 30.0, List.of(0, 1, 2));

    // add an empty topic with many partitions on only brokers 1 and 2
    roundRobinPartitionsOnBrokers(clusterModel, EMPTY_TOPIC, 1000, 0.0, List.of(1, 2));

    // We use DiskUsageDistributionGoal here, but any ResourceDistributionGoal would work
    final var diskUsageDistributionGoal = (DiskUsageDistributionGoal) AnalyzerUnitTestUtils.goal(DiskUsageDistributionGoal.class);
    final var optimizationOptions = new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    diskUsageDistributionGoal.initGoalState(clusterModel, optimizationOptions);

    // the disk usage goal is violated, but should still be acceptable to try to move the partitions of the
    // empty topic from broker 2 to broker 0 to balance the replica count
    final var toMovePartition = new TopicPartition(EMPTY_TOPIC, 0);
    final var balancingAction = new BalancingAction(toMovePartition, 2, 0, ActionType.INTER_BROKER_REPLICA_MOVEMENT);

    final var result = diskUsageDistributionGoal.actionAcceptance(balancingAction, clusterModel);
    assertEquals(ActionAcceptance.ACCEPT, result);
  }

  private void roundRobinPartitionsOnBrokers(
      ClusterModel clusterModel,
      String topicName,
      int numPartitions,
      double partitionLoad,
      List<Integer> targetBrokers
  ) {
    // add replicas of rf=2 topic in a round-robin fashion on the targetBrokers
    for (int i = 0; i < numPartitions; i++) {
      final var leaderBroker = targetBrokers.get(i % targetBrokers.size());
      final var followerBroker = targetBrokers.get((i + 1) % targetBrokers.size());
      final var tp = new TopicPartition(topicName, i);
      clusterModel.createReplica(rackId(leaderBroker), leaderBroker, tp, 0, true);
      clusterModel.createReplica(rackId(followerBroker), followerBroker, tp, 1, false);

      final var load = DeterministicCluster.createLoad(partitionLoad, partitionLoad, partitionLoad, partitionLoad);
      final var windows = Collections.singletonList(1L);
      clusterModel.setReplicaLoad(rackId(leaderBroker), leaderBroker, tp, load, windows);
      clusterModel.setReplicaLoad(rackId(followerBroker), followerBroker, tp, load, windows);
    }
  }

  private String rackId(int brokerId) {
    return RACK_BY_BROKER.get(brokerId).toString();
  }
}
