/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
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
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * Soft goal to move the leaders to the first replica of each partition.
 */
public class PreferredLeaderElectionGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(PreferredLeaderElectionGoal.class);
  private final ProvisionResponse _provisionResponse;
  private final boolean _skipUrpDemotion;
  private final boolean _excludeFollowerDemotion;
  private Cluster _kafkaCluster;

  public PreferredLeaderElectionGoal() {
    this(false, false, null);
  }

  public PreferredLeaderElectionGoal(boolean skipUrpDemotion,
                                     boolean excludeFollowerDemotion,
                                     Cluster kafkaCluster) {
    if (skipUrpDemotion && kafkaCluster == null) {
      throw new IllegalArgumentException("Cluster information is not provided.");
    }
    _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
    _skipUrpDemotion = skipUrpDemotion;
    _excludeFollowerDemotion = excludeFollowerDemotion;
    _kafkaCluster = kafkaCluster;
  }

  private void sanityCheckOptimizationOptions(OptimizationOptions optimizationOptions) {
    if (optimizationOptions.isTriggeredByGoalViolation()) {
      throw new IllegalArgumentException(String.format("%s goal does not support use by goal violation detector.", name()));
    }
  }

  private void maybeMoveReplicaToEndOfReplicaList(Replica replica, ClusterModel clusterModel) {
    // There are two scenarios where replica swap operation is skipped:
    // 1.the replica is not leader replica and _excludeFollowerDemotion is true.
    // 2.the replica's partition is currently under replicated and _skipUrpDemotion is true.
    if (!(_skipUrpDemotion && isPartitionUnderReplicated(_kafkaCluster, replica.topicPartition()))
        && !(_excludeFollowerDemotion && !replica.isLeader())) {
      Partition p = clusterModel.partition(replica.topicPartition());
      p.moveReplicaToEnd(replica);
    }
  }

  private void maybeChangeLeadershipForPartition(Set<Replica> leaderReplicas, Set<TopicPartition> partitionsToMove) {
    // If the leader replica's partition is currently under replicated and _skipUrpDemotion is true, skip leadership
    // change operation.
    leaderReplicas.stream()
                  .filter(r -> !(_skipUrpDemotion && isPartitionUnderReplicated(_kafkaCluster, r.topicPartition())))
                  .forEach(r -> partitionsToMove.add(r.topicPartition()));
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions) {
    sanityCheckOptimizationOptions(optimizationOptions);
    // First move the replica on the demoted brokers to the end of the replica list.
    // If all the replicas are demoted, no change is made to the leader.
    boolean hasBrokerOrDiskToBeDemoted = false;
    Set<TopicPartition> partitionsToMove = new HashSet<>();
    for (Broker b : clusterModel.aliveBrokers()) {
      if (b.isDemoted()) {
        hasBrokerOrDiskToBeDemoted = true;
        for (Replica r : b.replicas()) {
          maybeMoveReplicaToEndOfReplicaList(r, clusterModel);
        }
        maybeChangeLeadershipForPartition(b.leaderReplicas(), partitionsToMove);
      } else {
        for (Disk d : b.disks()) {
          if (d.state() == Disk.State.DEMOTED) {
            hasBrokerOrDiskToBeDemoted = true;
            for (Replica r : d.replicas()) {
              maybeMoveReplicaToEndOfReplicaList(r, clusterModel);
            }
            maybeChangeLeadershipForPartition(d.leaderReplicas(), partitionsToMove);
          }
        }
      }
    }
    // Check whether this goal has relocated any leadership.
    boolean relocatedLeadership = false;
    Set<Integer> excludedBrokersForLeadership = optimizationOptions.excludedBrokersForLeadership();
    // Ignore the excluded topics because this goal does not move partitions.
    for (List<Partition> partitions : clusterModel.getPartitionsByTopic().values()) {
      for (Partition p : partitions) {
        if (hasBrokerOrDiskToBeDemoted && !partitionsToMove.contains(p.topicPartition())) {
          continue;
        }
        for (int i = 0; i < p.replicas().size(); i++) {
          // If there is no broker or disk to be demoted, only try to transfer the leadership to the first replica of the partition.
          if (!hasBrokerOrDiskToBeDemoted && i > 0) {
            break;
          }
          Replica r = p.replicas().get(i);
          // Iterate over the replicas and ensure that (1) the leader is set to the first alive replica, and (2) the
          // leadership is not transferred to a broker excluded for leadership transfer.
          Broker leaderCandidate = r.broker();
          if (leaderCandidate.isAlive()) {
            if (r.isCurrentOffline()) {
              LOG.warn("The preferred replica of partition {} on broker {} is offline.", p.topicPartition(), leaderCandidate);
              continue;
            }
            if (!r.isLeader()) {
              if (excludedBrokersForLeadership.contains(leaderCandidate.id())) {
                LOG.warn("Skipped leadership transfer of partition {} to broker {} because it is among brokers excluded"
                         + " for leadership {}.", p.topicPartition(), leaderCandidate, excludedBrokersForLeadership);
                continue;
              }
              clusterModel.relocateLeadership(r.topicPartition(), p.leader().broker().id(), leaderCandidate.id());
              relocatedLeadership = true;
            }
            if (clusterModel.demotedBrokers().contains(leaderCandidate)) {
              LOG.warn("The leader of partition {} has to be on a demoted broker {} because all the alive "
                       + "replicas are demoted.", p.topicPartition(), leaderCandidate.id());
            }
            if (r.disk() != null && r.disk().state() == Disk.State.DEMOTED) {
              LOG.warn("The leader of partition {} has to be on a demoted disk {} of broker {} because all the alive "
                       + "replicas are demoted.", p.topicPartition(), r.disk().logDir(), leaderCandidate.id());
            }
            break;
          }
        }
      }
    }
    // This goal is optimized in one pass.
    finish();
    // Return true if at least one leadership has been relocated.
    return relocatedLeadership;
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
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0, true);
  }

  @Override
  public String name() {
    return PreferredLeaderElectionGoal.class.getSimpleName();
  }

  @Override
  public void finish() {
    _kafkaCluster = null;
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  @Override
  public ProvisionStatus provisionStatus() {
    // Provision status computation is not relevant to PLE goal.
    return provisionResponse().status();
  }

  @Override
  public ProvisionResponse provisionResponse() {
    return _provisionResponse;
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
