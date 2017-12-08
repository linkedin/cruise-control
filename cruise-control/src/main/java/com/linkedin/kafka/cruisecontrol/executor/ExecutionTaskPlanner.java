/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class holds the execution of balance proposals for rebalance.
 * <p>
 * Each proposal is assigned an execution id and managed in two ways.
 * <ul>
 * <li>All proposals of leader movements are put into the same list and will be executed together.
 * <li>Proposals of partition movements are tracked by broker and execution Id.
 * </ul>
 * <p>
 * This class tracks the execution proposals for each broker using a Map(id -> List[proposals])
 * The proposal is tracked both under source broker and
 * destination broker's plan.
 * Once a proposal is fulfilled, the proposal will be removed based on its execution ID from both
 * source broker and destination broker's execution plan.
 * <p>
 * This class is not thread safe.
 */
public class ExecutionTaskPlanner {
  private final Logger LOG = LoggerFactory.getLogger(ExecutionTaskPlanner.class);
  private final Map<Integer, Map<Long, ExecutionTask>> _partMoveProposalByBrokerId;
  private volatile long _dataToMove;
  private final Set<ExecutionTask> _remainingReplicaMovements;
  private final Map<Long, ExecutionTask> _leaderMovements;
  private final AtomicLong _executionId;

  public ExecutionTaskPlanner() {
    _executionId = new AtomicLong(0L);
    _partMoveProposalByBrokerId = new HashMap<>();
    _remainingReplicaMovements = new TreeSet<>();
    _dataToMove = 0L;
    _leaderMovements = new HashMap<>();
  }

  public void addBalancingProposals(Collection<BalancingProposal> proposals) {
    for (BalancingProposal proposal : proposals) {
      addBalancingProposal(proposal);
    }
  }

  /**
   * Add a new proposal that needs to be executed.
   *
   * @param proposal the proposal to execute.
   */
  public void addBalancingProposal(BalancingProposal proposal) {
    // Get the execution Id for this proposal;
    long proposalExecutionId = _executionId.getAndIncrement();
    ExecutionTask executionTask = new ExecutionTask(proposalExecutionId, proposal);
    // Check if partition movement is required.
    if (proposal.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
      _remainingReplicaMovements.add(executionTask);
      _dataToMove += proposal.dataToMove();
      // Add the proposal to source broker execution plan
      int sourceBroker = proposal.sourceBrokerId();
      Map<Long, ExecutionTask> sourceBrokerProposalMap = _partMoveProposalByBrokerId.get(sourceBroker);
      if (sourceBrokerProposalMap == null) {
        sourceBrokerProposalMap = new HashMap<>();
        _partMoveProposalByBrokerId.put(sourceBroker, sourceBrokerProposalMap);
      }
      sourceBrokerProposalMap.put(proposalExecutionId, executionTask);

      // Add the proposal to destination broker execution plan
      int destinationBroker = proposal.destinationBrokerId();
      Map<Long, ExecutionTask> destinationBrokerProposalMap = _partMoveProposalByBrokerId.get(destinationBroker);
      if (destinationBrokerProposalMap == null) {
        destinationBrokerProposalMap = new HashMap<>();
        _partMoveProposalByBrokerId.put(destinationBroker, destinationBrokerProposalMap);
      }
      destinationBrokerProposalMap.put(proposalExecutionId, executionTask);
    } else {
      // Only monitoredLeaderBrokerMetadata movement is required. No partition movement is required.
      _leaderMovements.put(proposalExecutionId, executionTask);
    }
    LOG.trace("Added balancing proposal {}", proposal);
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
    return _dataToMove;
  }

  /**
   * Get the remaining leader movements.
   */
  public Collection<ExecutionTask> remainingLeaderMovements() {
    return _leaderMovements.values();
  }

  public List<ExecutionTask> getLeaderMovementTasks(int numTasks) {
    List<ExecutionTask> leaderMovementsList = new ArrayList<>();
    Iterator<ExecutionTask> leaderMovementIter = _leaderMovements.values().iterator();
    for (int i = 0; i < numTasks && leaderMovementIter.hasNext(); i++) {
      leaderMovementsList.add(leaderMovementIter.next());
      leaderMovementIter.remove();
    }
    return leaderMovementsList;
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
        Map<Long, ExecutionTask> proposalsForBroker = _partMoveProposalByBrokerId.get(brokerId);
        LOG.trace("Execution task for broker {} are {}", brokerId, proposalsForBroker);
        if (proposalsForBroker != null) {
          for (Map.Entry<Long, ExecutionTask> taskEntry : proposalsForBroker.entrySet()) {
            ExecutionTask task = taskEntry.getValue();

            // Skip this proposal if either source broker or destination broker of this proposal has already
            // involved in this round.
            int sourceBroker = task.proposal.sourceBrokerId();
            int destinationBroker = task.proposal.destinationBrokerId();
            if (brokerInvolved.contains(sourceBroker) || brokerInvolved.contains(destinationBroker)) {
              continue;
            }
            TopicPartition tp = task.proposal.topicPartition();
            // Check if the proposal is executable.
            if (isExecutableProposal(task.proposal, readyBrokers)
                && !inProgressPartitions.contains(tp)
                && !partitionsInvolved.contains(tp)) {
              partitionsInvolved.add(tp);
              executableReplicaMovements.add(task);
              // Record the two brokers as involved in this round and stop involving them again in this round.
              brokerInvolved.add(sourceBroker);
              brokerInvolved.add(destinationBroker);
              // Remove the proposal from the execution plan.
              removeProposalForExecution(task);
              // Decrement the slots for both source and destination brokers
              readyBrokers.put(sourceBroker, readyBrokers.get(sourceBroker) - 1);
              readyBrokers.put(destinationBroker, readyBrokers.get(destinationBroker) - 1);
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
    _leaderMovements.clear();
    _partMoveProposalByBrokerId.clear();
    _remainingReplicaMovements.clear();
    _dataToMove = 0L;
  }

  /**
   * A proposal is executable if both source broker and destination broker is ready. i.e. has slots to execute more
   * proposals.
   */
  private boolean isExecutableProposal(BalancingProposal proposal, Map<Integer, Integer> readyBrokers) {
    int sourceBroker = proposal.sourceBrokerId();
    int destinationBroker = proposal.destinationBrokerId();
    return readyBrokers.get(sourceBroker) > 0 && readyBrokers.get(destinationBroker) > 0;
  }

  private void removeProposalForExecution(ExecutionTask task) {
    int sourceBroker = task.proposal.sourceBrokerId();
    int destinationBroker = task.proposal.destinationBrokerId();
    _partMoveProposalByBrokerId.get(sourceBroker).remove(task.executionId);
    _partMoveProposalByBrokerId.get(destinationBroker).remove(task.executionId);
    _remainingReplicaMovements.remove(task);
    _dataToMove -= task.proposal.dataToMove();
  }

}
