/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.LEADER_ACTION;


/**
 * The class holds the execution of balance proposals for rebalance.
 * <p>
 * Each proposal is assigned an execution id and managed in two ways.
 * <ul>
 * <li>All proposals of leadership movements are put into the same list and will be executed together.
 * <li>Proposals of partition movements are tracked by broker and execution Id.
 * </ul>
 * <p>
 * This class tracks the execution proposals for each broker using a sorted Set. The proposal's position in this set
 * represents its execution order and is determined by the {@link ExecutionTaskPlanner#_replicaMovementTaskStrategy}.
 * The proposal is tracked both under source broker and destination broker's plan.
 * Once a proposal is fulfilled, the proposal will be removed from both source broker and destination broker's execution plan.
 * <p>
 * This class is not thread safe.
 */
public class ExecutionTaskPlanner {
  private final Logger LOG = LoggerFactory.getLogger(ExecutionTaskPlanner.class);
  private Map<Integer, SortedSet<ExecutionTask>> _partMoveTaskByBrokerId;
  private long _remainingDataToMove;
  private final Set<ExecutionTask> _remainingReplicaMovements;
  private final Map<Long, ExecutionTask> _remainingLeadershipMovements;
  private long _executionId;
  private ReplicaMovementStrategy _replicaMovementTaskStrategy;

  public ExecutionTaskPlanner(List<String> replicaMovementStrategies) {
    _executionId = 0L;
    _partMoveTaskByBrokerId = new HashMap<>();
    _remainingReplicaMovements = new TreeSet<>();
    _remainingDataToMove = 0L;
    _remainingLeadershipMovements = new HashMap<>();
    if (replicaMovementStrategies == null || replicaMovementStrategies.isEmpty()) {
      _replicaMovementTaskStrategy = new BaseReplicaMovementStrategy();
    } else {
      for (String replicaMovementStrategy : replicaMovementStrategies) {
        try {
          if (_replicaMovementTaskStrategy == null) {
            _replicaMovementTaskStrategy = (ReplicaMovementStrategy) Class.forName(replicaMovementStrategy).newInstance();
          } else {
            _replicaMovementTaskStrategy = _replicaMovementTaskStrategy.chain(
                (ReplicaMovementStrategy) Class.forName(replicaMovementStrategy).newInstance());
          }
        } catch (Exception e) {
          throw new RuntimeException("Error occurred while setting up the replica movement strategy: " + replicaMovementStrategy + ".", e);
        }
      }
      // Chain the custom strategies with BaseReplicaMovementStrategy in the end to handle the scenario that provided custom strategy is unable
      // to determine the order of two tasks. BaseReplicaMovementStrategy makes the task with smaller execution id to get executed first.
      _replicaMovementTaskStrategy = _replicaMovementTaskStrategy.chain(new BaseReplicaMovementStrategy());
    }
  }

  /**
   * Add each given proposal to execute, unless the given cluster state indicates that the proposal would be a no-op.
   * A proposal is a no-op if the expected state after the execution of the given proposal is the current cluster state.
   * The proposal to be added will have at least one of the two following actions:
   * 1. Replica action (i.e. movement, addition, deletion or order change).
   * 2. Leader action (i.e. leadership movement)
   *
   * @param proposals Execution proposals.
   * @param cluster Kafka cluster state.
   */
  public void addExecutionProposals(Collection<ExecutionProposal> proposals, Cluster cluster) {
    LOG.trace("Cluster state before adding proposals: {}.", cluster);
    maybeAddReplicaMovementTasks(proposals, cluster);
    maybeAddLeaderChangeTasks(proposals, cluster);
  }

  /**
   * For each proposal, create a replica action task if there is a need for moving replica(s) to reach expected final proposal state.
   *
   * @param proposals Execution proposals.
   * @param cluster Kafka cluster state.
   */
  private void maybeAddReplicaMovementTasks(Collection<ExecutionProposal> proposals, Cluster cluster) {
    for (ExecutionProposal proposal : proposals) {
      TopicPartition tp = proposal.topicPartition();
      PartitionInfo partitionInfo = cluster.partition(tp);
      if (partitionInfo == null) {
        LOG.trace("Ignored the attempt to move non-existing partition for topic partition: {}", tp);
        continue;
      }
      if (!proposal.isCompletedSuccessfully(partitionInfo.replicas())) {
        long replicaActionExecutionId = _executionId++;
        ExecutionTask executionTask = new ExecutionTask(replicaActionExecutionId, proposal, REPLICA_ACTION);
        _remainingReplicaMovements.add(executionTask);
        _remainingDataToMove += proposal.dataToMoveInMB();
        LOG.trace("Added action {} as replica proposal {}", replicaActionExecutionId, proposal);
      }
    }
    _partMoveTaskByBrokerId = _replicaMovementTaskStrategy.applyStrategy(_remainingReplicaMovements, cluster);
  }

  /**
   * For each proposal, create a leader action task if there is a need for moving the leadership to reach expected final proposal state.
   *
   * @param proposals Execution proposals.
   * @param cluster Kafka cluster state.
   */
  private void maybeAddLeaderChangeTasks(Collection<ExecutionProposal> proposals, Cluster cluster) {
    for (ExecutionProposal proposal : proposals) {
      if (proposal.hasLeaderAction()) {
        Node currentLeader = cluster.leaderFor(proposal.topicPartition());
        if (currentLeader != null && currentLeader.id() != proposal.newLeader()) {
          // Get the execution Id for the leader action proposal execution;
          long leaderActionExecutionId = _executionId++;
          ExecutionTask leaderActionTask = new ExecutionTask(leaderActionExecutionId, proposal, LEADER_ACTION);
          _remainingLeadershipMovements.put(leaderActionExecutionId, leaderActionTask);
          LOG.trace("Added action {} as leader proposal {}", leaderActionExecutionId, proposal);
        }
      }
    }
  }

  /**
   * Get the remaining replica movement tasks.
   */
  public Set<ExecutionTask> remainingReplicaMovements() {
    return _remainingReplicaMovements;
  }

  /**
   * Get the remaining data to move in MB.
   */
  public long remainingDataToMoveInMB() {
    return _remainingDataToMove;
  }

  /**
   * Get the remaining leadership movements.
   */
  public Collection<ExecutionTask> remainingLeadershipMovements() {
    return _remainingLeadershipMovements.values();
  }

  /**
   * Get the leadership movement tasks, and remove them from _remainingLeadershipMovements.
   *
   * @param numTasks Number of tasks to remove from the _remainingLeadershipMovements. If _remainingLeadershipMovements
   *                 has less than numTasks, all tasks are removed.
   */
  public List<ExecutionTask> getLeadershipMovementTasks(int numTasks) {
    List<ExecutionTask> leadershipMovementsList = new ArrayList<>();
    Iterator<ExecutionTask> leadershipMovementIter = _remainingLeadershipMovements.values().iterator();
    for (int i = 0; i < numTasks && leadershipMovementIter.hasNext(); i++) {
      leadershipMovementsList.add(leadershipMovementIter.next());
      leadershipMovementIter.remove();
    }
    return leadershipMovementsList;
  }

  /**
   * Get a list of executable replica movements that comply with the concurrency constraint.
   *
   * @param readyBrokers The brokers that is ready to execute more movements.
   * @param inProgressPartitions Topic partitions of replicas that are already in progress. This is needed because the
   *                             controller does not allow updating the ongoing replica reassignment for a partition
   *                             whose replica is being reassigned.
   * @return A list of movements that is executable for the ready brokers.
   */
  public List<ExecutionTask> getReplicaMovementTasks(Map<Integer, Integer> readyBrokers,
                                                       Set<TopicPartition> inProgressPartitions) {
    LOG.trace("Getting tasks for brokers with concurrency {}", readyBrokers);
    List<ExecutionTask> executableReplicaMovements = new ArrayList<>();
    /**
     * The algorithm avoids unfair situation where the available movement slots of a broker is completely taken
     * by another broker. It checks the proposals in a round-robin manner that makes sure each ready broker gets
     * chances to make progress.
     */
    boolean newTaskAdded = true;
    Set<Integer> brokerInvolved = new HashSet<>();
    Set<TopicPartition> partitionsInvolved = new HashSet<>();
    while (newTaskAdded) {
      newTaskAdded = false;
      brokerInvolved.clear();
      for (Map.Entry<Integer, Integer> brokerEntry : readyBrokers.entrySet()) {
        int brokerId = brokerEntry.getKey();
        // If this broker has already involved in this round, skip it.
        if (brokerInvolved.contains(brokerId)) {
          continue;
        }

        // Check the available balancing proposals of this broker to see if we can find one ready to execute.
        SortedSet<ExecutionTask> proposalsForBroker = _partMoveTaskByBrokerId.get(brokerId);
        LOG.trace("Execution task for broker {} are {}", brokerId, proposalsForBroker);
        if (proposalsForBroker != null) {
          for (ExecutionTask task : proposalsForBroker) {
            // Skip this proposal if either source broker or destination broker of this proposal has already
            // involved in this round.
            int sourceBroker = task.proposal().oldLeader();
            Set<Integer> destinationBrokers = task.proposal().replicasToAdd();
            if (brokerInvolved.contains(sourceBroker)
                || KafkaCruiseControlUtils.containsAny(brokerInvolved, destinationBrokers)) {
              continue;
            }
            TopicPartition tp = task.proposal().topicPartition();
            // Check if the proposal is executable.
            if (isExecutableProposal(task.proposal(), readyBrokers)
                && !inProgressPartitions.contains(tp)
                && !partitionsInvolved.contains(tp)) {
              partitionsInvolved.add(tp);
              executableReplicaMovements.add(task);
              // Record the two brokers as involved in this round and stop involving them again in this round.
              brokerInvolved.add(sourceBroker);
              brokerInvolved.addAll(destinationBrokers);
              // Remove the proposal from the execution plan.
              removeReplicaActionForExecution(task);
              // Decrement the slots for both source and destination brokers
              readyBrokers.put(sourceBroker, readyBrokers.get(sourceBroker) - 1);
              for (int broker : destinationBrokers) {
                readyBrokers.put(broker, readyBrokers.get(broker) - 1);
              }
              // Mark proposal added to true so we will have another round of check.
              newTaskAdded = true;
              LOG.debug("Found ready task {} for broker {}. Broker concurrency state: {}", task, brokerId, readyBrokers);
              // We can stop the check for proposals for this broker because we have found a proposal.
              break;
            }
          }
        }
      }
    }
    return executableReplicaMovements;
  }

  /**
   * Clear all the states.
   */
  public void clear() {
    _remainingLeadershipMovements.clear();
    _partMoveTaskByBrokerId.clear();
    _remainingReplicaMovements.clear();
    _remainingDataToMove = 0L;
  }

  /**
   * A proposal is executable if the source broker and all the destination brokers are ready. i.e. has slots to
   * execute more proposals.
   */
  private boolean isExecutableProposal(ExecutionProposal proposal, Map<Integer, Integer> readyBrokers) {
    if (readyBrokers.get(proposal.oldLeader()) <= 0) {
      return false;
    }
    for (int destinationBroker : proposal.replicasToAdd()) {
      if (readyBrokers.get(destinationBroker) <= 0) {
        return false;
      }
    }
    return true;
  }

  private void removeReplicaActionForExecution(ExecutionTask task) {
    int sourceBroker = task.proposal().oldLeader();
    _partMoveTaskByBrokerId.get(sourceBroker).remove(task);
    for (int destinationBroker : task.proposal().replicasToAdd()) {
      _partMoveTaskByBrokerId.get(destinationBroker).remove(task);
    }
    _remainingReplicaMovements.remove(task);
    _remainingDataToMove -= task.proposal().dataToMoveInMB();
  }

}
