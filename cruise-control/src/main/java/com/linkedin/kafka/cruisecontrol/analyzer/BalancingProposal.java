/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import org.apache.kafka.common.TopicPartition;


/**
 * Represents the load balancing operation over a replica for Kafka Load GoalOptimizer.
 */
public class BalancingProposal {
  private final TopicPartition _topicPartition;
  private final int _sourceBrokerId;
  private final int _destinationBrokerId;
  private final BalancingAction _balancingAction;
  private final long _dataToMove;

  /**
   * Constructor for creating a balancing proposal with given topic partition, source broker and destination id, and
   * balancing action.
   *
   * @param topicPartition      Topic partition of the replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param balancingAction     Leadership transfer or replica relocation.
   */
  public BalancingProposal(TopicPartition topicPartition, int sourceBrokerId, int destinationBrokerId,
                           BalancingAction balancingAction) {
    this(topicPartition, sourceBrokerId, destinationBrokerId, balancingAction, 0L);
  }

  /**
   * Constructor for creating a balancing proposal with given topic partition, source broker and destination id, and
   * balancing action.
   *
   * @param topicPartition      Topic partition of the replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param balancingAction     Leadership transfer or replica relocation.
   * @param dataToMove          The data to move with this proposal. The unit should be MB.
   */
  public BalancingProposal(TopicPartition topicPartition,
                           int sourceBrokerId,
                           int destinationBrokerId,
                           BalancingAction balancingAction,
                           long dataToMove) {
    _topicPartition = topicPartition;
    _sourceBrokerId = sourceBrokerId;
    _destinationBrokerId = destinationBrokerId;
    _balancingAction = balancingAction;
    _dataToMove = dataToMove;
  }

  /**
   * Get the partition Id that is impacted by the balancing action.
   */
  public int partitionId() {
    return _topicPartition.partition();
  }

  /**
   * Get topic name of the impacted partition.
   */
  public String topic() {
    return _topicPartition.topic();
  }

  /**
   * Get topic partition.
   */
  public TopicPartition topicPartition() {
    return _topicPartition;
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

  /**
   * Get string representation of this balancing proposal.
   */
  @Override
  public String toString() {
    return String.format("(%s, %d->%d, %s)", _topicPartition, _sourceBrokerId, _destinationBrokerId, _balancingAction);
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
        && _destinationBrokerId == otherProposal._destinationBrokerId && _topicPartition.equals(otherProposal._topicPartition)
        && _balancingAction == otherProposal._balancingAction;
  }

  @Override
  public int hashCode() {
    int result = _topicPartition.hashCode();
    result = 31 * result + _sourceBrokerId;
    result = 31 * result + _destinationBrokerId;
    result = 31 * result + _balancingAction.hashCode();
    return result;
  }
}
