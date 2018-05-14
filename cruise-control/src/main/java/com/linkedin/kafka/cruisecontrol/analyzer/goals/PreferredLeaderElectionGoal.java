/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
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
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;


/**
 * This is a goal that simply move the leaders to the first replica of each partition.
 */
public class PreferredLeaderElectionGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(PreferredLeaderElectionGoal.class);

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics)
      throws KafkaCruiseControlException {
    // First move the replica on the demoted brokers to the end of the replica list.
    // If all the replicas are demoted, no change is made to the leader.
    Set<TopicPartition> partitionsToMove = new HashSet<>();
    for (Broker b : clusterModel.demotedBrokers()) {
      for (Replica r : b.replicas()) {
        Partition p = clusterModel.partition(r.topicPartition());
        p.moveReplicaToEnd(r);
      }
      b.leaderReplicas().forEach(r -> partitionsToMove.add(r.topicPartition()));
    }
    // Ignore the excluded topics because this goal does not move partitions.
    for (List<Partition> partitions : clusterModel.getPartitionsByTopic().values()) {
      for (Partition p : partitions) {
        if (!clusterModel.demotedBrokers().isEmpty() && !partitionsToMove.contains(p.topicPartition())) {
          continue;
        }
        for (Replica r : p.replicas()) {
          // Iterate over the replicas and ensure the leader is set to the first alive replica.
          if (r.broker().isAlive()) {
            if (!r.isLeader()) {
              clusterModel.relocateLeadership(r.topicPartition(), p.leader().broker().id(), r.broker().id());
            }
            if (clusterModel.demotedBrokers().contains(r.broker())) {
              LOG.warn("The leader of partition {} has to be on a demoted broker {} because all the alive "
                           + "replicas are demoted.", p.topicPartition(), r.broker());
            }
            break;
          }
        }
      }
    }
    return true;
  }

  /**
   * @deprecated
   * Please use {@link #actionAcceptance(BalancingAction, ClusterModel)} instead.
   */
  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return true;
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
        return "This goals do not care about stats.";
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
