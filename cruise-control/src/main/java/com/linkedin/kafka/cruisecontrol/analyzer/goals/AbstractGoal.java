/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import java.util.SortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;


/**
 * An abstract class for goals. This class will be extended to crete custom goals for different purposes -- e.g.
 * balancing the distribution of replicas or resources in the cluster.
 */
public abstract class AbstractGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractGoal.class);
  private boolean _finished;
  protected boolean _succeeded = true;
  protected BalancingConstraint _balancingConstraint;
  protected int _numSnapshots = 1;
  protected double _minMonitoredPartitionPercentage = 0.995;

  /**
   * Constructor of Abstract Goal class sets the _finished flag to false to signal that the goal requirements have not
   * been satisfied, yet.
   */
  public AbstractGoal() {
    _finished = false;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(configs, false));
    String numSnapshotString = (String) configs.get(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    if (numSnapshotString != null && !numSnapshotString.isEmpty()) {
      _numSnapshots = Integer.parseInt(numSnapshotString);
    }
    String minMonitoredPartitionPercentageString =
        (String) configs.get(KafkaCruiseControlConfig.MIN_MONITORED_PARTITION_PERCENTAGE_CONFIG);
    if (minMonitoredPartitionPercentageString != null
        && !minMonitoredPartitionPercentageString.isEmpty()) {
      _minMonitoredPartitionPercentage = Double.parseDouble(minMonitoredPartitionPercentageString);
    }
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException, OptimizationFailureException {
    _succeeded = true;
    LOG.debug("Starting optimization for {}.", name());
    // Initialize pre-optimized stats.
    ClusterModelStats statsBeforeOptimization = clusterModel.getClusterStats(_balancingConstraint);
    LOG.trace("[PRE - {}] {}", name(), statsBeforeOptimization);
    _finished = false;
    long goalStartTime = System.currentTimeMillis();
    initGoalState(clusterModel, excludedTopics);
    Collection<Broker> deadBrokers = clusterModel.deadBrokers();

    while (!_finished) {
      for (Broker broker : brokersToBalance(clusterModel)) {
        rebalanceForBroker(broker, clusterModel, optimizedGoals, excludedTopics);
      }
      updateGoalState(clusterModel, excludedTopics);
    }
    ClusterModelStats statsAfterOptimization = clusterModel.getClusterStats(_balancingConstraint);
    LOG.trace("[POST - {}] {}", name(), statsAfterOptimization);
    LOG.debug("Finished optimization for {} in {}ms.", name(), System.currentTimeMillis() - goalStartTime);
    LOG.trace("Cluster after optimization is {}", clusterModel);
    // We only ensure the optimization did not make stats worse when it is not self-healing.
    if (deadBrokers.isEmpty()) {
      ClusterModelStatsComparator comparator = clusterModelStatsComparator();
      // Throw exception when the stats before optimization is preferred.
      if (comparator.compare(statsAfterOptimization, statsBeforeOptimization) < 0) {
        throw new OptimizationFailureException("Optimization for Goal " + name() + " failed because the optimized"
                                                   + "result is worse than before. Detail reason: "
                                                   + comparator.explainLastComparison());
      }
    }
    return _succeeded;
  }

  @Override
  public abstract boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel);

  @Override
  public abstract String name();

  /**
   * Check whether the replica should be excluded from the rebalance. A replica should be excluded if its topic
   * is in the excluded topics set and its broker is still alive.
   * @param replica the replica to check.
   * @param excludedTopics the excluded topics set.
   * @return true if the replica should be excluded, false otherwise.
   */
  protected boolean shouldExclude(Replica replica, Set<String> excludedTopics) {
    return excludedTopics.contains(replica.topicPartition().topic()) && replica.originalBroker().isAlive();
  }

  /**
   * Get sorted brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  protected abstract SortedSet<Broker> brokersToBalance(ClusterModel clusterModel);

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param proposal     Proposal containing information about
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  protected abstract boolean selfSatisfied(ClusterModel clusterModel, BalancingProposal proposal);

  /**
   * Signal for finishing the process for rebalance or self-healing for this goal.
   */
  protected void finish() {
    _finished = true;
  }

  /**
   * (1) Initialize states that this goal requires -- e.g. in TopicReplicaDistributionGoal and ReplicaDistributionGoal,
   * this method is used to populate the ReplicaDistributionTarget(s). (2) Run sanity checks regarding minimum
   * requirements of hard goals.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  protected abstract void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException;

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposal.
   */
  protected abstract void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws AnalysisInputException, OptimizationFailureException;

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  protected abstract void rebalanceForBroker(Broker broker,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             Set<String> excludedTopics)
      throws AnalysisInputException, ModelInputException, OptimizationFailureException;

  /**
   * Attempt to apply the given balancing action to the given replica in the given cluster. The application
   * considers the candidate brokers as the potential destination brokers for replica movement or the location of
   * followers for leadership transfer. If the movement attempt succeeds, the function returns the broker id of the
   * destination, otherwise the function returns null.
   *
   * @param clusterModel    The state of the cluster.
   * @param replica         Replica to be applied the given balancing action.
   * @param candidateBrokers Candidate brokers as the potential destination brokers for replica movement or the location
   *                        of followers for leadership transfer.
   * @param action          Balancing action.
   * @param optimizedGoals  Optimized goals.
   * @return Broker id of the destination if the movement attempt succeeds, null otherwise.
   */
  protected Broker maybeApplyBalancingAction(ClusterModel clusterModel,
                                              Replica replica,
                                              Collection<Broker> candidateBrokers,
                                              BalancingAction action,
                                              Set<Goal> optimizedGoals)
      throws ModelInputException, AnalysisInputException {
    // In self healing mode, allow a move only from dead to alive brokers.
    if (!clusterModel.deadBrokers().isEmpty() && replica.originalBroker().isAlive()) {
      //return null;
      LOG.trace("Applying {} to a replica in a healthy broker in self-healing mode.", action);
    }
    Collection<Broker> eligibleBrokers = getEligibleBrokers(clusterModel, replica, candidateBrokers);
    for (Broker broker : eligibleBrokers) {
      BalancingProposal optimizedGoalProposal =
          new BalancingProposal(replica.topicPartition(), replica.broker().id(), broker.id(), action);
      // A replica should be moved if:
      // 0. The move is legit.
      // 1. The goal requirements are not violated if this proposal is applied to the given cluster state.
      // 2. The movement is acceptable by the previously optimized goals.
      boolean canMove = legitMove(replica, broker, action);
      canMove = canMove && selfSatisfied(clusterModel, optimizedGoalProposal);
      canMove = canMove && AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, optimizedGoalProposal, clusterModel);
      LOG.trace("Trying to apply balancing action {}, legitMove = {}, selfSatisfied = {}, satisfyOptimizedGoals = {}",
          optimizedGoalProposal, legitMove(replica, broker, action), selfSatisfied(clusterModel, optimizedGoalProposal),
          AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, optimizedGoalProposal, clusterModel));
      if (canMove) {
        if (action == BalancingAction.LEADERSHIP_MOVEMENT) {
          clusterModel.relocateLeadership(replica.topicPartition(), replica.broker().id(), broker.id());
        } else if (action == BalancingAction.REPLICA_MOVEMENT) {
          clusterModel.relocateReplica(replica.topicPartition(), replica.broker().id(), broker.id());
        }
        return broker;
      }
    }
    return null;
  }

  private boolean legitMove(Replica replica, Broker destBroker, BalancingAction balancingAction) {
    if (balancingAction == BalancingAction.REPLICA_MOVEMENT && destBroker.replica(replica.topicPartition()) == null) {
      return true;
    } else if (balancingAction == BalancingAction.LEADERSHIP_MOVEMENT && replica.isLeader()
        && destBroker.replica(replica.topicPartition()) != null) {
      return true;
    }
    return false;
  }

  private Collection<Broker> getEligibleBrokers(ClusterModel clusterModel,
                                                Replica replica,
                                                Collection<Broker> candidateBrokers) {
    if (clusterModel.newBrokers().isEmpty()) {
      return candidateBrokers;
    } else {
      List<Broker> eligibleBrokers = new ArrayList<>();
      // When there are new brokers, we should only allow the replicas to be moved to the new brokers.
      candidateBrokers.forEach(b -> {
        if (b.isNew() || b == replica.originalBroker()) {
          eligibleBrokers.add(b);
        }
      });
      return eligibleBrokers;
    }
  }

  @Override
  public String toString() {
    return name();
  }
}
