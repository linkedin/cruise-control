/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */
package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicLeadershipDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC3;
import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TopicLeadershipDistributionGoalTest {

    private static final int NUM_RACKS = 3;
    private static final int NUM_BROKERS = 6;

    @Test
    public void testOptimize() throws KafkaCruiseControlException {
        ClusterModel clusterModel = generateClusterModel();

        Goal goal = initializeGoal(new TopicLeadershipDistributionGoal());
        goal.optimize(
                clusterModel,
                Collections.emptySet(),
                new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));

        Map<String, List<Partition>> partitionsByTopic = clusterModel.getPartitionsByTopic();

        for (String topic : Arrays.asList(TOPIC0, TOPIC1, TOPIC2, TOPIC3)) {
            Map<Broker, Integer> leaderCountsByBroker = new HashMap<>();

            for (Partition partition : partitionsByTopic.get(topic)) {
                leaderCountsByBroker.compute(
                        partition.leader().broker(),
                        (broker, count) -> count == null ? 1 : count + 1);
            }

            int numPartitions = partitionsByTopic.get(topic).size();
            int floorNumLeadPartitionsPerBroker = Math.floorDiv(numPartitions, NUM_BROKERS);
            int expectedNumPlusOneBrokers = numPartitions % NUM_BROKERS;

            int numPlusOneBrokers = 0;

            for (Integer numLeadPartitions : leaderCountsByBroker.values()) {

                if (numLeadPartitions == floorNumLeadPartitionsPerBroker + 1) {
                    numPlusOneBrokers++;
                } else if (numLeadPartitions != floorNumLeadPartitionsPerBroker) {
                    fail(String.format(
                            "Expected %s or %s lead partitions for topic %s but got %s: %s",
                            floorNumLeadPartitionsPerBroker,
                            floorNumLeadPartitionsPerBroker + 1,
                            topic,
                            numLeadPartitions,
                            leaderCountsByBroker));
                }
            }

            System.out.println(topic);
            assertEquals(expectedNumPlusOneBrokers, numPlusOneBrokers);
        }
    }

    @Test
    public void testActionAcceptance() throws KafkaCruiseControlException {
        ClusterModel clusterModel = generateClusterModel();

        // TopicLeadershipDistributionGoal assumes RackAwareGoal has already run.
        Goal rackAwareGoal = initializeGoal(new RackAwareGoal());
        rackAwareGoal.optimize(
                clusterModel,
                Collections.emptySet(),
                new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));

        Goal goal = initializeGoal(new TopicLeadershipDistributionGoal());
        goal.optimize(
                clusterModel,
                Set.of(rackAwareGoal),
                new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));

        Map<Integer, Integer> topic0LeaderCountByBroker = new HashMap<>();
        for (Partition partition : clusterModel.getPartitionsByTopic().get(TOPIC0)) {
            topic0LeaderCountByBroker.compute(
                    partition.leader().broker().id(),
                    (brokerId, leaderCount) -> leaderCount == null ? 1 : leaderCount + 1);
        }

        Broker topic0OccupiedBroker = clusterModel.aliveBrokers().stream()
                .filter(b -> topic0LeaderCountByBroker.containsKey(b.id()))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Expected an occupied broker for topic0"));
        Broker topic0OtherOccupiedBroker = clusterModel.aliveBrokers().stream()
                .filter(b -> topic0LeaderCountByBroker.containsKey(b.id()) && !b.equals(topic0OccupiedBroker))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Expected another occupied broker for topic0"));
        Broker topic0UnoccupiedBroker = clusterModel.aliveBrokers().stream()
                .filter(b -> !topic0LeaderCountByBroker.containsKey(b.id()))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Expected an unoccupied broker for topic0"));

        TopicPartition topic0Partition = topic0OccupiedBroker.replicasOfTopicInBroker(TOPIC0).stream()
                .filter(Replica::isLeader)
                .findAny()
                .orElseThrow(() -> new RuntimeException(
                        "Expected at least one replica for topic0 on broker " + topic0OccupiedBroker.id()))
                .topicPartition();

        // ACCEPT: Move a lead replica of topic0 from an occupied broker to an unoccupied broker (occupied brokers are
        //         essentially +1 brokers)

        ActionAcceptance accept1 = goal.actionAcceptance(
                new BalancingAction(
                        topic0Partition,
                        topic0OccupiedBroker.id(),
                        topic0UnoccupiedBroker.id(),
                        ActionType.INTER_BROKER_REPLICA_MOVEMENT),
                clusterModel);

        assertEquals(ActionAcceptance.ACCEPT, accept1);

        // REPLICA_REJECT: Move a lead replica of topic0 from an occupied broker to another occupied broker (occupied
        //                 brokers are essentially +1 brokers)

        ActionAcceptance reject1 = goal.actionAcceptance(
                new BalancingAction(
                        topic0Partition,
                        topic0OccupiedBroker.id(),
                        topic0OtherOccupiedBroker.id(),
                        ActionType.INTER_BROKER_REPLICA_MOVEMENT),
                clusterModel);

        assertEquals(ActionAcceptance.REPLICA_REJECT, reject1);

        // REPLICA_REJECT: Move any lead replica of topic3

        TopicPartition topic3Partition = topic0OccupiedBroker.replicasOfTopicInBroker(TOPIC3).stream()
                .filter(Replica::isLeader)
                .findAny()
                .orElseThrow(() -> new RuntimeException(
                        "Expected at least one replica for topic3 on broker " + topic0OccupiedBroker.id()))
                .topicPartition();

        ActionAcceptance reject2 = goal.actionAcceptance(
                new BalancingAction(
                        topic3Partition,
                        1,
                        3,
                        ActionType.INTER_BROKER_REPLICA_MOVEMENT),
                clusterModel);

        assertEquals(ActionAcceptance.REPLICA_REJECT, reject2);
    }

    private Goal initializeGoal(Goal goal) {
        // These two need to be set but the goal doesn't actually use it so we're setting them to empty strings here
        goal.configure(Map.of(BOOTSTRAP_SERVERS_CONFIG, "", ZOOKEEPER_CONNECT_CONFIG, ""));

        return goal;
    }

    private ClusterModel generateClusterModel() {
        ClusterModel clusterModel = new ClusterModel(
                new ModelGeneration(0, 0),
                1.0);

        for (int i = 0; i < NUM_RACKS; i++) {
            clusterModel.createRack("r" + i);
        }

        BrokerCapacityInfo capacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY);

        List<Broker> brokers = new ArrayList<>();

        for (int i = 1; i <= NUM_BROKERS; i++) {
            String rack = "r" + (i % NUM_RACKS);
            String host = "h" + i;

            brokers.add(clusterModel.createBroker(rack, host, i, capacityInfo, false));
        }

        List<TopicPartition> topicPartitions = Arrays.asList(
                // This topic should result in 3 brokers at +1 lead replicas
                new TopicPartition(TOPIC0, 0),
                new TopicPartition(TOPIC0, 1),
                new TopicPartition(TOPIC0, 2),

                // This topic should result in a single broker at +1 lead replicas
                new TopicPartition(TOPIC1, 0),
                new TopicPartition(TOPIC1, 1),
                new TopicPartition(TOPIC1, 2),
                new TopicPartition(TOPIC1, 3),
                new TopicPartition(TOPIC1, 4),
                new TopicPartition(TOPIC1, 5),
                new TopicPartition(TOPIC1, 6),

                // This topic should result in 2 brokers at +1 lead replicas
                new TopicPartition(TOPIC2, 0),
                new TopicPartition(TOPIC2, 1),
                new TopicPartition(TOPIC2, 2),
                new TopicPartition(TOPIC2, 3),
                new TopicPartition(TOPIC2, 4),
                new TopicPartition(TOPIC2, 5),
                new TopicPartition(TOPIC2, 6),
                new TopicPartition(TOPIC2, 7),

                // This topic should also be perfectly distributed (all brokers at +0 lead replicas)
                new TopicPartition(TOPIC3, 0),
                new TopicPartition(TOPIC3, 1),
                new TopicPartition(TOPIC3, 2),
                new TopicPartition(TOPIC3, 3),
                new TopicPartition(TOPIC3, 4),
                new TopicPartition(TOPIC3, 5),
                new TopicPartition(TOPIC3, 6),
                new TopicPartition(TOPIC3, 7),
                new TopicPartition(TOPIC3, 8),
                new TopicPartition(TOPIC3, 9),
                new TopicPartition(TOPIC3, 10),
                new TopicPartition(TOPIC3, 11)
        );

        // Bring all topics to RF=3, all with replica sets of {1, 2, 3}
        for (TopicPartition tp : topicPartitions) {
            for (int i = 0; i < 3; i++) {
                clusterModel.createReplica(
                        brokers.get(i).rack().id(),
                        brokers.get(i).id(),
                        tp,
                        i,
                        i == 0,
                        false,
                        null,
                        false);

                MetricValues metricValues = new MetricValues(1);
                Map<Short, MetricValues> metricValuesByResource = new HashMap<>();
                Resource.cachedValues().forEach(r -> {
                    for (short id : KafkaMetricDef.resourceToMetricIds(r)) {
                        metricValuesByResource.put(id, metricValues);
                    }
                });
                clusterModel.setReplicaLoad(
                        brokers.get(i).rack().id(),
                        brokers.get(i).id(),
                        tp,
                        new AggregatedMetricValues(metricValuesByResource),
                        Collections.singletonList(1L));
            }
        }

        return clusterModel;
    }
}
