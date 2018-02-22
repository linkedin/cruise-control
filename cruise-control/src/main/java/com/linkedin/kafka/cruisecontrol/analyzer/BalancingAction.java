/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import java.util.HashMap;
import java.util.Map;


/**
 * Represents the load balancing operation over a replica for Kafka Load GoalOptimizer.
 */
public class BalancingAction {
  private final TopicPartition _tp;
  private final Integer _sourceBrokerId;
  private final Integer _destinationBrokerId;
  private final ActionType _actionType;
  private final TopicPartition _destinationTp;

  /**
   * Constructor for creating a balancing proposal with given topic partition, source broker and destination id, and
   * balancing action.
   *
   * @param tp                  Topic partition of the replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param actionType          Action type.
   */
  public BalancingAction(TopicPartition tp,
                         Integer sourceBrokerId,
                         Integer destinationBrokerId,
                         ActionType actionType) {
    this(tp, sourceBrokerId, destinationBrokerId, actionType, tp);
  }

  /**
   * Constructor for creating a balancing proposal with given topic partitions, source and destination broker id,
   * and the topic partition of replica to swap with.
   *
   * @param sourceTp            Topic partition of the source replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param actionType          Action type.
   * @param destinationTp       Topic partition of the replica to swap with.
   */
  public BalancingAction(TopicPartition sourceTp,
                         Integer sourceBrokerId,
                         Integer destinationBrokerId,
                         ActionType actionType,
                         TopicPartition destinationTp) {
    _tp = sourceTp;
    _sourceBrokerId = sourceBrokerId;
    _destinationBrokerId = destinationBrokerId;
    _actionType = actionType;
    _destinationTp = destinationTp;
    validate();
  }

  private void validate() {
    switch (_actionType) {
      case REPLICA_ADDITION:
        if (_destinationBrokerId == null) {
          throw new IllegalArgumentException("The destination broker cannot be null for balancing action " + _actionType);
        } else if (_sourceBrokerId != null) {
          throw new IllegalArgumentException("The source broker should be null for balancing action " + _actionType);
        }
        break;
      case REPLICA_DELETION:
        if (_destinationBrokerId != null) {
          throw new IllegalArgumentException("The destination broker should be null for balancing action " + _actionType);
        } else if (_sourceBrokerId == null) {
          throw new IllegalArgumentException("The source broker cannot be null for balancing action " + _actionType);
        }
        break;
      case REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
      case REPLICA_SWAP:
        if (_destinationBrokerId == null) {
          throw new IllegalArgumentException("The destination broker cannot be null for balancing action " + _actionType);
        } else if (_sourceBrokerId == null) {
          throw new IllegalArgumentException("The source broker cannot be null for balancing action " + _actionType);
        }
        break;
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  /**
   * Get the destination topic partition to swap with.
   */
  public TopicPartition destinationTopicPartition() {
    return _destinationTp;
  }

  /**
   * Get topic name of the replica to swap with at the destination.
   */
  public String destinationTopic() {
    return _destinationTp.topic();
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
  public Integer sourceBrokerId() {
    return _sourceBrokerId;
  }

  /**
   * Get the destination broker Id.
   */
  public Integer destinationBrokerId() {
    return _destinationBrokerId;
  }

  /**
   * Get the type of action that provides balancing.
   */
  public ActionType balancingAction() {
    return _actionType;
  }

  /**
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> proposalMap = new HashMap<>();
    proposalMap.put("topicPartition", _tp);
    proposalMap.put("sourceBrokerId", _sourceBrokerId);
    proposalMap.put("destinationBrokerId", _destinationBrokerId);
    proposalMap.put("destinationTopicPartition", _destinationTp);
    proposalMap.put("actionType", _actionType);
    return proposalMap;
  }

  /**
   * Get string representation of this balancing proposal.
   */
  @Override
  public String toString() {
    String actSymbol = _actionType.equals(ActionType.REPLICA_SWAP) ? "<->" : "->";
    return String.format("(%s%s%s, %d%s%d, %s)",
                         _tp, actSymbol, _destinationTp, _sourceBrokerId, actSymbol, _destinationBrokerId, _actionType);
  }

  /**
   * Compare the given object with this object.
   *
   * @param other Other object to be compared with this object.
   * @return True if other object equals this object, false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BalancingAction)) {
      return false;
    }

    if (this == other) {
      return true;
    }

    BalancingAction otherAction = (BalancingAction) other;
    if (_sourceBrokerId == null) {
      if (otherAction._sourceBrokerId != null) {
        return false;
      }
    } else if (!_sourceBrokerId.equals(otherAction._sourceBrokerId)) {
      return false;
    }

    if (_destinationBrokerId == null) {
      if (otherAction._destinationBrokerId != null) {
        return false;
      }
    } else if (!_destinationBrokerId.equals(otherAction._destinationBrokerId)) {
      return false;
    }

    return _tp.equals(otherAction._tp) && _actionType == otherAction._actionType && _destinationTp.equals(otherAction._destinationTp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_tp, _sourceBrokerId, _destinationBrokerId, _actionType, _destinationTp);
  }
}
