/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;


/**
 * The execution proposal corresponding to a particular partition.
 */
public class ExecutionProposal {
  private final TopicPartition _tp;
  private final long _partitionSize;
  private final int _oldLeader;
  private final List<Integer> _oldReplicas;
  private final List<Integer> _newReplicas;
  private final Set<Integer> _replicasToAdd;
  private final Set<Integer> _replicasToRemove;

  /**
   * Construct an execution proposals.
   * Note that the new leader will be the first replica in the new replica list after the proposal is executed.
   *
   * @param tp the topic partition of this execution proposal
   * @param partitionSize the size of the partition.
   * @param oldLeader the old leader of the partition to determine if leader movement is needed.
   * @param oldReplicas the old replicas for rollback. (Rollback is not supported until KAFKA-6034)
   * @param newReplicas the new replicas of the partition in this order.
   */
  public ExecutionProposal(TopicPartition tp,
                           long partitionSize,
                           int oldLeader,
                           List<Integer> oldReplicas,
                           List<Integer> newReplicas) {
    _tp = tp;
    _partitionSize = partitionSize;
    _oldLeader = oldLeader;
    // Allow the old replicas to be empty for partition addition.
    _oldReplicas = oldReplicas == null ? Collections.emptyList() : oldReplicas;
    _newReplicas = newReplicas;
    validate();
    // Populate added replicas
    _replicasToAdd = new HashSet<>();
    _replicasToAdd.addAll(newReplicas);
    _replicasToAdd.removeAll(_oldReplicas);
    // Populate removed replicas
    _replicasToRemove = new HashSet<>();
    _replicasToRemove.addAll(_oldReplicas);
    _replicasToRemove.removeAll(newReplicas);
  }

  private boolean orderMatched(Node[] currentOrderedReplicas, List<Integer> replicas) {
    if (replicas.size() != currentOrderedReplicas.length) {
      return false;
    }

    for (int i = 0; i < replicas.size(); i++) {
      if (currentOrderedReplicas[i].id() != replicas.get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check whether the successful proposal completion is reflected in the current ordered replicas in the given cluster.
   *
   * @param currentOrderedReplicas Current ordered replica list from the cluster.
   * @return True if successfully completed, false otherwise.
   */
  public boolean isCompletedSuccessfully(Node[] currentOrderedReplicas) {
    return orderMatched(currentOrderedReplicas, _newReplicas);
  }

  /**
   * Check whether the proposal abortion is reflected in the current ordered replicas in the given cluster.
   * There could be a race condition that when we abort a task, it is already completed.
   * In that case, we treat it as aborted as well.
   *
   * @param currentOrderedReplicas Current ordered replica list from the cluster.
   * @return True if aborted, false otherwise.
   */
  public boolean isAborted(Node[] currentOrderedReplicas) {
    return isCompletedSuccessfully(currentOrderedReplicas) || orderMatched(currentOrderedReplicas, _oldReplicas);
  }

  /**
   * @return the topic for this proposal.
   */
  public String topic() {
    return _tp.topic();
  }

  /**
   * @return the partition id for this proposal.
   */
  public int partitionId() {
    return _tp.partition();
  }

  /**
   * @return the TopicPartition of this proposal.
   */
  public TopicPartition topicPartition() {
    return _tp;
  }

  /**
   * @return the partition size of the partition this proposal is about.
   */
  public long partitionSize() {
    return _partitionSize;
  }

  /**
   * @return the old leader of the partition before the executing the proposal.
   */
  public int oldLeader() {
    return _oldLeader;
  }

  /**
   * @return the new leader of the partition after executing the proposal.
   */
  public int newLeader() {
    return _newReplicas.get(0);
  }

  /**
   * @return the replica list of the partition before executing the proposal.
   */
  public List<Integer> oldReplicas() {
    return _oldReplicas;
  }

  /**
   * @return the new replica list fo the partition after executing the proposal.
   */
  public List<Integer> newReplicas() {
    return _newReplicas;
  }

  /**
   * @return the replicas that exist in new replica list but not in old replica list.
   */
  public Set<Integer> replicasToAdd() {
    return _replicasToAdd;
  }

  /**
   * @return the replicas that exist in old replica list but not in the new replica list.
   */
  public Set<Integer> replicasToRemove() {
    return _replicasToRemove;
  }

  /**
   * @return whether the proposal involves a replica action. (replica movement, addition, deletion, reorder)
   */
  public boolean hasReplicaAction() {
    return !_oldReplicas.equals(_newReplicas);
  }

  /**
   * @return whether the proposal involves a leader action. i.e. leader movement.
   */
  public boolean hasLeaderAction() {
    return _oldLeader != _newReplicas.get(0);
  }

  /**
   * @return the total number of bytes to move involved in this proposal.
   */
  public long dataToMoveInMB() {
    return _replicasToAdd.size() * _partitionSize;
  }

  private void validate() {
    // Verify old leader exists.
    if (_oldLeader >= 0 && !_oldReplicas.contains(_oldLeader)) {
      throw new IllegalArgumentException(String.format("The old leader %d does not exit in the old replica list %s",
                                                       _oldLeader, _oldReplicas));
    }
    // verify empty new replica list.
    if (_newReplicas == null || _newReplicas.isEmpty()) {
      throw new IllegalArgumentException("The new replica list " + _newReplicas + " cannot be empty.");
    }
    // Verify duplicates
    Set<Integer> checkSet = new HashSet<>();
    checkSet.addAll(_newReplicas);
    if (checkSet.size() != _newReplicas.size()) {
      throw new IllegalArgumentException("The new replicas list " + _newReplicas + " has duplicate replica.");
    }
  }

  /**
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> proposalMap = new HashMap<>();
    proposalMap.put("topicPartition", _tp);
    proposalMap.put("oldLeader", _oldLeader);
    proposalMap.put("oldReplicas", _oldReplicas);
    proposalMap.put("newReplicas", _newReplicas);
    return proposalMap;
  }

  @Override
  public String toString() {
    return String.format("{%s, oldLeader: %d, %s -> %s}", _tp, _oldLeader, _oldReplicas, _newReplicas);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ExecutionProposal)) {
      return false;
    }

    if (this == other) {
      return true;
    }

    ExecutionProposal otherProposal = (ExecutionProposal) other;

    return _tp.equals(otherProposal._tp)
        && _oldLeader == otherProposal._oldLeader
        && _oldReplicas.equals(otherProposal._oldReplicas)
        && _newReplicas.equals(otherProposal._newReplicas);
  }

  @Override
  public int hashCode() {
    int result = _tp.hashCode();
    result = 31 * result + _oldLeader;
    for (int broker : _oldReplicas) {
      result = 31 * result + broker;
    }
    for (int broker : _newReplicas) {
      result = 31 * broker;
    }
    return result;
  }
}
