/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Collections;
import java.util.Set;


/**
 * A class to indicate options intended to be used during optimization of goals.
 */
public class OptimizationOptions {
  private final Set<String> _excludedTopics;
  private final Set<Integer> _excludedBrokersForLeadership;
  private final Set<Integer> _excludedBrokersForReplicaMove;
  private final boolean _isTriggeredByGoalViolation;
  private final Set<Integer> _requestedDestinationBrokerIds;
  private final boolean _onlyMoveImmigrantReplicas;

  /**
   * Default value for {@link #_excludedBrokersForLeadership} is an empty set.
   */
  public OptimizationOptions(Set<String> excludedTopics) {
    this(excludedTopics, Collections.emptySet());
  }

  /**
   * Default value for {@link #_excludedBrokersForReplicaMove} is an empty set.
   */
  public OptimizationOptions(Set<String> excludedTopics, Set<Integer> excludedBrokersForLeadership) {
    this(excludedTopics, excludedBrokersForLeadership, Collections.emptySet());
  }

  /**
   * Default value for {@link #_isTriggeredByGoalViolation} is false.
   */
  public OptimizationOptions(Set<String> excludedTopics,
                             Set<Integer> excludedBrokersForLeadership,
                             Set<Integer> excludedBrokersForReplicaMove) {
    this(excludedTopics, excludedBrokersForLeadership, excludedBrokersForReplicaMove, false);
  }

  /**
   * Default value for {@link #_requestedDestinationBrokerIds} is an empty set.
   */
  public OptimizationOptions(Set<String> excludedTopics,
                             Set<Integer> excludedBrokersForLeadership,
                             Set<Integer> excludedBrokersForReplicaMove,
                             boolean isTriggeredByGoalViolation) {
    this(excludedTopics, excludedBrokersForLeadership, excludedBrokersForReplicaMove, isTriggeredByGoalViolation, Collections.emptySet());
  }

  /**
   * Default value for {@link #_onlyMoveImmigrantReplicas} is false.
   */
  public OptimizationOptions(Set<String> excludedTopics,
                             Set<Integer> excludedBrokersForLeadership,
                             Set<Integer> excludedBrokersForReplicaMove,
                             boolean isTriggeredByGoalViolation,
                             Set<Integer> requestedDestinationBrokerIds) {
    this(excludedTopics, excludedBrokersForLeadership, excludedBrokersForReplicaMove, isTriggeredByGoalViolation,
         requestedDestinationBrokerIds, false);
  }

  public OptimizationOptions(Set<String> excludedTopics,
                             Set<Integer> excludedBrokersForLeadership,
                             Set<Integer> excludedBrokersForReplicaMove,
                             boolean isTriggeredByGoalViolation,
                             Set<Integer> requestedDestinationBrokerIds,
                             boolean onlyMoveImmigrantReplicas) {
    _excludedTopics = excludedTopics;
    _excludedBrokersForLeadership = excludedBrokersForLeadership;
    _excludedBrokersForReplicaMove = excludedBrokersForReplicaMove;
    _isTriggeredByGoalViolation = isTriggeredByGoalViolation;
    _requestedDestinationBrokerIds = requestedDestinationBrokerIds;
    _onlyMoveImmigrantReplicas = onlyMoveImmigrantReplicas;
  }

  public Set<String> excludedTopics() {
    return Collections.unmodifiableSet(_excludedTopics);
  }

  public Set<Integer> excludedBrokersForLeadership() {
    return Collections.unmodifiableSet(_excludedBrokersForLeadership);
  }

  public Set<Integer> excludedBrokersForReplicaMove() {
    return Collections.unmodifiableSet(_excludedBrokersForReplicaMove);
  }

  public boolean isTriggeredByGoalViolation() {
    return _isTriggeredByGoalViolation;
  }

  public Set<Integer> requestedDestinationBrokerIds() {
    return Collections.unmodifiableSet(_requestedDestinationBrokerIds);
  }

  public boolean onlyMoveImmigrantReplicas() {
    return _onlyMoveImmigrantReplicas;
  }

  @Override
  public String toString() {
    return String.format("[excludedTopics=%s,excludedBrokersForLeadership=%s,excludedBrokersForReplicaMove=%s,"
                         + "isTriggeredByGoalViolation=%s,requestedDestinationBrokerIds=%s,onlyMoveImmigrantReplicas=%s]",
                         _excludedTopics, _excludedBrokersForLeadership, _excludedBrokersForReplicaMove, _isTriggeredByGoalViolation,
                         _requestedDestinationBrokerIds, _onlyMoveImmigrantReplicas);
  }
}
