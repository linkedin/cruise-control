/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import org.apache.kafka.common.TopicPartition;
import java.util.HashMap;
import java.util.Map;


/**
 * Represents the load balancing operation over a replica for Kafka Load GoalOptimizer.
 */
public class BalancingProposal {
  private final TopicPartition _tp;
  private final int _sourceBrokerId;
  private final int _destinationBrokerId;
  private final BalancingAction _balancingAction;
  private final long _dataToMove;

  /**
   * Constructor for creating a balancing proposal with given topic partition, source broker and destination id, and
   * balancing action.
   *
   * @param tp                  Topic partition of the replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param balancingAction     Leadership transfer or replica relocation.
   */
  public BalancingProposal(TopicPartition tp, int sourceBrokerId, int destinationBrokerId,
                           BalancingAction balancingAction) {
    this(tp, sourceBrokerId, destinationBrokerId, balancingAction, 0L);
  }

  /**
   * Constructor for creating a balancing proposal with given topic partition, source broker and destination id, and
   * balancing action.
   *
   * @param tp                  Topic partition of the replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param balancingAction     Leadership transfer or replica relocation.
   * @param dataToMove          The data to move with this proposal. The unit should be MB.
   */
  public BalancingProposal(TopicPartition tp,
                           int sourceBrokerId,
                           int destinationBrokerId,
                           BalancingAction balancingAction,
                           long dataToMove) {
    _tp = tp;
    _sourceBrokerId = sourceBrokerId;
    _destinationBrokerId = destinationBrokerId;
    _balancingAction = balancingAction;
    _dataToMove = dataToMove;
  }

  /**
   * Get the partition Id that is impacted by the balancing action.
   */
  public int partitionId() {
    return _tp.partition();
  }

  /**
   * Get topic name of the impacted partition.
   */
  public String topic() {
    return _tp.topic();
  }

  /**
   * Get topic partition.
   */
  public TopicPartition topicPartition() {
    return _tp;
  }

  /**
   * Get the source broker id that is impacted by the balancing action.
   */
  public int sourceBrokerId() {
    return _sourceBrokerId;
  }

  /**
   * Get the destination broker Id.
   */
  public int destinationBrokerId() {
    return _destinationBrokerId;
  }

  /**
   * Get the type of action that provides balancing.
   */
  public BalancingAction balancingAction() {
    return _balancingAction;
  }

  /**
   * Get the data to move with the proposal. The unit should be MB.
   */
  public long dataToMove() {
    return _dataToMove;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> proposalMap = new HashMap<>();
    proposalMap.put("topicPartition", _tp);
    proposalMap.put("sourceBrokerId", _sourceBrokerId);
    proposalMap.put("destinationBrokerId", _destinationBrokerId);
    proposalMap.put("balancingAction", _balancingAction);
    return proposalMap;
  }

  /**
   * Get string representation of this balancing proposal.
   */
  @Override
  public String toString() {
    return String.format("(%s, %d->%d, %s)", _tp, _sourceBrokerId, _destinationBrokerId, _balancingAction);
  }

  /**
   * Compare the given object with this object.
   *
   * @param other Other object to be compared with this object.
   * @return True if other object equals this object, false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BalancingProposal)) {
      return false;
    }

    if (this == other) {
      return true;
    }

    BalancingProposal otherProposal = (BalancingProposal) other;
    return _sourceBrokerId == otherProposal._sourceBrokerId
        && _destinationBrokerId == otherProposal._destinationBrokerId && _tp.equals(otherProposal._tp)
        && _balancingAction == otherProposal._balancingAction;
  }

  @Override
  public int hashCode() {
    int result = _tp.hashCode();
    result = 31 * result + _sourceBrokerId;
    result = 31 * result + _destinationBrokerId;
    result = 31 * result + _balancingAction.hashCode();
    return result;
  }
}
