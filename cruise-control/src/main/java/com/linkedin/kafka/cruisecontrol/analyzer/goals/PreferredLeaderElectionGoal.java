/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.isPartitionUnderReplicated;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;


/**
 * This is a goal that simply move the leaders to the first replica of each partition.
 */
public class PreferredLeaderElectionGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(PreferredLeaderElectionGoal.class);
  private final boolean _skipUrpDemotion;
  private final boolean _excludeFollowerDemotion;
  private final Cluster _kafkaCluster;

  public PreferredLeaderElectionGoal() {
    this(false, false, null);
  }

  public PreferredLeaderElectionGoal(boolean skipUrpDemotion,
                                     boolean excludeFollowerDemotion,
                                     Cluster kafkaCluster) {
    if (skipUrpDemotion && kafkaCluster == null) {
      throw new IllegalArgumentException("Cluster information is not provided.");
    }
    _skipUrpDemotion = skipUrpDemotion;
    _excludeFollowerDemotion = excludeFollowerDemotion;
    _kafkaCluster = kafkaCluster;
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions) {
    // First move the replica on the demoted brokers to the end of the replica list.
    // If all the replicas are demoted, no change is made to the leader.
    Set<TopicPartition> partitionsToMove = new HashSet<>();
    for (Broker b : clusterModel.demotedBrokers()) {
      for (Replica r : b.replicas()) {
        // There are two scenarios where replica swap operation is skipped:
        // 1.the replica is not leader replica and _excludeFollowerDemotion is true.
        // 2.the replica's partition is currently under replicated and _skipUrpDemotion is true.
        if (!(_skipUrpDemotion && isPartitionUnderReplicated(_kafkaCluster, r.topicPartition()))
            && !(_excludeFollowerDemotion && !r.isLeader())) {
          Partition p = clusterModel.partition(r.topicPartition());
          p.moveReplicaToEnd(r);
        }
      }
      // If the leader replica's partition is currently under replicated and _skipUrpDemotion is true, skip leadership
      // change operation.
      b.leaderReplicas().stream().filter(r -> !(_skipUrpDemotion && isPartitionUnderReplicated(_kafkaCluster, r.topicPartition())))
       .forEach(r -> partitionsToMove.add(r.topicPartition()));
    }
    // Check whether this goal has relocated any leadership.
    boolean relocatedLeadership = false;
    Set<Integer> excludedBrokersForLeadership = optimizationOptions.excludedBrokersForLeadership();
    // Ignore the excluded topics because this goal does not move partitions.
    for (List<Partition> partitions : clusterModel.getPartitionsByTopic().values()) {
      for (Partition p : partitions) {
        if (!clusterModel.demotedBrokers().isEmpty() && !partitionsToMove.contains(p.topicPartition())) {
          continue;
        }
        for (Replica r : p.replicas()) {
          // Iterate over the replicas and ensure that (1) the leader is set to the first alive replica, and (2) the
          // leadership is not transferred to a broker excluded for leadership transfer.
          Broker leaderCandidate = r.broker();
          if (leaderCandidate.isAlive()) {
            if (!r.isLeader()) {
              if (excludedBrokersForLeadership.contains(leaderCandidate.id())) {
                LOG.warn("Skipped leadership transfer of partition {} to broker {} because it is among brokers excluded"
                         + " for leadership {}.", p.topicPartition(), leaderCandidate);
                continue;
              }
              clusterModel.relocateLeadership(r.topicPartition(), p.leader().broker().id(), leaderCandidate.id());
              relocatedLeadership = true;
            }
            if (clusterModel.demotedBrokers().contains(leaderCandidate)) {
              LOG.warn("The leader of partition {} has to be on a demoted broker {} because all the alive "
                       + "replicas are demoted.", p.topicPartition(), leaderCandidate);
            }
            break;
          }
        }
      }
    }
    // Return true if at least one leadership has been relocated.
    return relocatedLeadership;
  }

  /**
   * @deprecated
   * Please use {@link #optimize(ClusterModel, Set, OptimizationOptions)} instead.
   */
  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics) {
    return optimize(clusterModel, optimizedGoals, new OptimizationOptions(excludedTopics));
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    return ACCEPT;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ClusterModelStatsComparator() {
      @Override
      public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
        return 0;
      }

      @Override
      public String explainLastComparison() {
        return String.format("Comparison for the %s is irrelevant.", name());
      }
    };
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, 0, true);
  }

  @Override
  public String name() {
    return PreferredLeaderElectionGoal.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
