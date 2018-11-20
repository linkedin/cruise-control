/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.*;


public class PreferredLeaderElectionGoalTest {
  private static final String TOPIC0 = "topic0";
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";

  private static final TopicPartition T0P0 = new TopicPartition(TOPIC0, 0);
  private static final TopicPartition T0P1 = new TopicPartition(TOPIC0, 1);
  private static final TopicPartition T0P2 = new TopicPartition(TOPIC0, 2);

  private static final TopicPartition T1P0 = new TopicPartition(TOPIC1, 0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC1, 1);
  private static final TopicPartition T1P2 = new TopicPartition(TOPIC1, 2);

  private static final TopicPartition T2P0 = new TopicPartition(TOPIC2, 0);
  private static final TopicPartition T2P1 = new TopicPartition(TOPIC2, 1);
  private static final TopicPartition T2P2 = new TopicPartition(TOPIC2, 2);

  private static final int NUM_RACKS = 4;

  @Test
  public void testOptimize() throws KafkaCruiseControlException {
    ClusterModel clusterModel = createClusterModel();
    Cluster cluster = createCluster(false);

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, cluster);
    goal.optimize(clusterModel, Collections.emptySet(), Collections.emptySet());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        List<Replica> replicas = clusterModel.partition(new TopicPartition(t, p)).replicas();
        for (int i = 0; i < 3; i++) {
          // only the first replica should be leader.
          assertEquals(i == 0, replicas.get(i).isLeader());
        }
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokers() throws KafkaCruiseControlException {
    ClusterModel clusterModel = createClusterModel();
    Cluster cluster = createCluster(false);
    clusterModel.setBrokerState(0, Broker.State.DEMOTED);

    Set<TopicPartition> leaderPartitionsOnDemotedBroker = new HashSet<>();
    clusterModel.broker(0).leaderReplicas().forEach(r -> leaderPartitionsOnDemotedBroker.add(r.topicPartition()));
    Map<TopicPartition, Integer> leaderDistributionBeforeBrokerDemotion = new HashMap<>();
    clusterModel.brokers().forEach(b -> {
      b.leaderReplicas().forEach(r -> leaderDistributionBeforeBrokerDemotion.put(r.topicPartition(), b.id()));
    });

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, cluster);
    goal.optimize(clusterModel, Collections.emptySet(), Collections.emptySet());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        if (!leaderPartitionsOnDemotedBroker.contains(tp)) {
          int oldLeaderBroker = leaderDistributionBeforeBrokerDemotion.get(tp);
          assertEquals("Tp " + tp, oldLeaderBroker, clusterModel.partition(tp).leader().broker().id());
        } else {
          List<Replica> replicas = clusterModel.partition(tp).replicas();
          for (int i = 0; i < 3; i++) {
            Replica replica = replicas.get(i);
            // only the first replica should be leader.
            assertEquals(i == 0, replica.isLeader());
            if (clusterModel.broker(0).replicas().contains(replica)) {
              // The demoted replica should be in the last position.
              assertEquals(replicas.size() - 1, i);
            }
          }
        }
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokersAndSkipUrpDemotion() throws KafkaCruiseControlException {
    ClusterModel clusterModel = createClusterModel();
    Cluster cluster = createCluster(true);
    clusterModel.setBrokerState(1, Broker.State.DEMOTED);

    Map<TopicPartition, Integer> leaderDistributionBeforeBrokerDemotion = new HashMap<>();
    clusterModel.brokers().forEach(b -> {
      b.leaderReplicas().forEach(r -> leaderDistributionBeforeBrokerDemotion.put(r.topicPartition(), b.id()));
    });

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(true, false, cluster);
    goal.optimize(clusterModel, Collections.emptySet(), Collections.emptySet());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        int oldLeaderBroker = leaderDistributionBeforeBrokerDemotion.get(tp);
        // All the demotion operation should be skipped since all the partitions are currently under replicated.
        assertEquals("Tp " + tp, oldLeaderBroker, clusterModel.partition(tp).leader().broker().id());
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokersAndExcludeFollowerDemotion() throws KafkaCruiseControlException {
    ClusterModel clusterModel = createClusterModel();
    Cluster cluster = createCluster(false);
    clusterModel.setBrokerState(2, Broker.State.DEMOTED);

    Set<TopicPartition> leaderPartitionsOnDemotedBroker = new HashSet<>();
    Map<TopicPartition, List<Replica>> originalReplicaListByTopicPartition = new HashMap<>();
    for (Replica r: clusterModel.broker(2).replicas()) {
      TopicPartition tp = r.topicPartition();
      if (r.isLeader()) {
        leaderPartitionsOnDemotedBroker.add(tp);
      } else {
        originalReplicaListByTopicPartition.put(tp, clusterModel.partition(tp).replicas());
      }
    }

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, true, cluster);
    goal.optimize(clusterModel, Collections.emptySet(), Collections.emptySet());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        if (leaderPartitionsOnDemotedBroker.contains(tp)) {
          List<Replica> replicas = clusterModel.partition(tp).replicas();
          // Only the first replica should be leader.
          assertSame(clusterModel.partition(tp).leader(), replicas.get(0));
          // The demoted replica should be in the last position.
          assertEquals(2, replicas.get(replicas.size() - 1).broker().id());
        } else if (originalReplicaListByTopicPartition.containsKey(tp)) {
          List<Replica> replicas = clusterModel.partition(tp).replicas();
          List<Replica> originalReplicas = originalReplicaListByTopicPartition.get(tp);
          // For partitions whose follower replicas are on the demote broker, swap operation should be skipped.
          assertEquals(replicas, originalReplicas);
        }
      }
    }
  }

  private ClusterModel createClusterModel() {

    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0),
                                                 1.0);
    for (int i = 0; i < NUM_RACKS; i++) {
      clusterModel.createRack("r" + i);
    }
    BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY);
    int i = 0;
    for (; i < 2; i++) {
      clusterModel.createBroker("r0", "h" + i, i, commonBrokerCapacityInfo);
    }
    for (int j = 1; j < NUM_RACKS; j++, i++) {
      clusterModel.createBroker("r" + j, "h" + i, i, commonBrokerCapacityInfo);
    }

    createReplicaAndSetLoad(clusterModel, "r0", 0, T0P0, 0, true);
    createReplicaAndSetLoad(clusterModel, "r0", 1, T0P1, 0, true);
    createReplicaAndSetLoad(clusterModel, "r1", 2, T0P2, 0, true);
    createReplicaAndSetLoad(clusterModel, "r2", 3, T1P0, 0, false);
    createReplicaAndSetLoad(clusterModel, "r3", 4, T1P1, 0, false);
    createReplicaAndSetLoad(clusterModel, "r0", 0, T1P2, 0, false);
    createReplicaAndSetLoad(clusterModel, "r0", 1, T2P0, 0, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, T2P1, 0, false);
    createReplicaAndSetLoad(clusterModel, "r2", 3, T2P2, 0, false);

    createReplicaAndSetLoad(clusterModel, "r3", 4, T0P0, 1, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, T0P1, 1, false);
    createReplicaAndSetLoad(clusterModel, "r0", 0, T0P2, 1, false);
    createReplicaAndSetLoad(clusterModel, "r0", 1, T1P0, 1, true);
    createReplicaAndSetLoad(clusterModel, "r2", 3, T1P1, 1, true);
    createReplicaAndSetLoad(clusterModel, "r3", 4, T1P2, 1, true);
    createReplicaAndSetLoad(clusterModel, "r1", 2, T2P0, 1, false);
    createReplicaAndSetLoad(clusterModel, "r0", 0, T2P1, 1, false);
    createReplicaAndSetLoad(clusterModel, "r0", 1, T2P2, 1, false);

    createReplicaAndSetLoad(clusterModel, "r2", 3, T0P0, 2, false);
    createReplicaAndSetLoad(clusterModel, "r3", 4, T0P1, 2, false);
    createReplicaAndSetLoad(clusterModel, "r2", 3, T0P2, 2, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, T1P0, 2, false);
    createReplicaAndSetLoad(clusterModel, "r0", 0, T1P1, 2, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, T1P2, 2, false);
    createReplicaAndSetLoad(clusterModel, "r3", 4, T2P0, 2, true);
    createReplicaAndSetLoad(clusterModel, "r2", 3, T2P1, 2, true);
    createReplicaAndSetLoad(clusterModel, "r3", 4, T2P2, 2, true);

    return clusterModel;
  }

  private void createReplicaAndSetLoad(ClusterModel clusterModel,
                                       String rack,
                                       int brokerId,
                                       TopicPartition tp,
                                       int index,
                                       boolean isLeader) {
    clusterModel.createReplica(rack, brokerId, tp, index, isLeader);
    MetricValues metricValues = new MetricValues(1);
    Map<Integer, MetricValues> metricValuesByResource = new HashMap<>();
    Resource.cachedValues().forEach(r -> {
      for (int id : KafkaMetricDef.resourceToMetricIds(r)) {
        metricValuesByResource.put(id, metricValues);
      }
    });
    clusterModel.setReplicaLoad(rack, brokerId, tp, new AggregatedMetricValues(metricValuesByResource),
                                Collections.singletonList(1L));
  }

  private Cluster createCluster(boolean isURP) {
    Node [] nodes = new Node [NUM_RACKS + 1];
    for (int i = 0; i < NUM_RACKS + 1; i++) {
      nodes[i] = new Node(i, "h" + i, 100);
    }
    List<PartitionInfo> partitions = new ArrayList<>(9);
    partitions.add(new PartitionInfo(T0P0.topic(), T0P0.partition(), nodes[0],
                   isURP ? new Node[]{nodes[0], nodes[4]} : new Node[]{nodes[0], nodes[4], nodes[3]},
                   new Node[]{nodes[0], nodes[4], nodes[3]}));

    partitions.add(new PartitionInfo(T0P1.topic(), T0P1.partition(), nodes[1],
                   isURP ? new Node[]{nodes[1], nodes[2]} : new Node[]{nodes[1], nodes[2], nodes[4]},
                   new Node[]{nodes[1], nodes[2], nodes[4]}));

    partitions.add(new PartitionInfo(T0P2.topic(), T0P2.partition(), nodes[2],
                   isURP ? new Node[]{nodes[2], nodes[0]} : new Node[]{nodes[2], nodes[0], nodes[3]},
                   new Node[]{nodes[2], nodes[0], nodes[3]}));

    partitions.add(new PartitionInfo(T1P0.topic(), T1P0.partition(), nodes[1],
                   isURP ? new Node[]{nodes[1], nodes[3]} : new Node[]{nodes[1], nodes[3], nodes[2]},
                   new Node[]{nodes[1], nodes[3], nodes[2]}));

    partitions.add(new PartitionInfo(T1P1.topic(), T1P1.partition(), nodes[3],
                   isURP ? new Node[]{nodes[3], nodes[4]} : new Node[]{nodes[3], nodes[4], nodes[0]},
                   new Node[]{nodes[3], nodes[4], nodes[0]}));

    partitions.add(new PartitionInfo(T1P2.topic(), T1P2.partition(), nodes[4],
                   isURP ? new Node[]{nodes[4], nodes[2]} : new Node[]{nodes[4], nodes[2], nodes[0]},
                   new Node[]{nodes[4], nodes[2], nodes[0]}));

    partitions.add(new PartitionInfo(T2P0.topic(), T2P0.partition(), nodes[4],
                   isURP ? new Node[]{nodes[4], nodes[2]} : new Node[]{nodes[4], nodes[2], nodes[1]},
                   new Node[]{nodes[4], nodes[2], nodes[1]}));

    partitions.add(new PartitionInfo(T2P1.topic(), T2P1.partition(), nodes[3],
                   isURP ? new Node[]{nodes[3], nodes[0]} : new Node[]{nodes[3], nodes[0], nodes[2]},
                   new Node[]{nodes[3], nodes[0], nodes[2]}));

    partitions.add(new PartitionInfo(T2P2.topic(), T2P2.partition(), nodes[4],
                   isURP ? new Node[]{nodes[4], nodes[1]} : new Node[]{nodes[4], nodes[1], nodes[3]},
                   new Node[]{nodes[4], nodes[1], nodes[3]}));

    return new Cluster("id", Arrays.asList(nodes), partitions, Collections.emptySet(), Collections.emptySet());
  }
}
