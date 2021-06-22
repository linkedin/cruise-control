/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Collections;
import java.util.Set;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


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
  private final boolean _fastMode;

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

  /**
   * Default value for {@link #_fastMode} is {@code true}.
   */
  public OptimizationOptions(Set<String> excludedTopics,
                             Set<Integer> excludedBrokersForLeadership,
                             Set<Integer> excludedBrokersForReplicaMove,
                             boolean isTriggeredByGoalViolation,
                             Set<Integer> requestedDestinationBrokerIds,
                             boolean onlyMoveImmigrantReplicas) {
    this(excludedTopics, excludedBrokersForLeadership, excludedBrokersForReplicaMove, isTriggeredByGoalViolation,
         requestedDestinationBrokerIds, onlyMoveImmigrantReplicas, true);
  }

  /**
   * The optimization options intended to be used during optimization of goals.
   */
  public OptimizationOptions(Set<String> excludedTopics,
                             Set<Integer> excludedBrokersForLeadership,
                             Set<Integer> excludedBrokersForReplicaMove,
                             boolean isTriggeredByGoalViolation,
                             Set<Integer> requestedDestinationBrokerIds,
                             boolean onlyMoveImmigrantReplicas,
                             boolean fastMode) {
    _excludedTopics = validateNotNull(excludedTopics, "Excluded topics cannot be null.");
    _excludedBrokersForLeadership = validateNotNull(excludedBrokersForLeadership, "Excluded brokers for leadership cannot be null.");
    _excludedBrokersForReplicaMove = validateNotNull(excludedBrokersForReplicaMove, "Excluded brokers for replica move cannot be null.");
    _isTriggeredByGoalViolation = isTriggeredByGoalViolation;
    _requestedDestinationBrokerIds = validateNotNull(requestedDestinationBrokerIds, "Requested destination broker ids cannot be null.");
    _onlyMoveImmigrantReplicas = onlyMoveImmigrantReplicas;
    _fastMode = fastMode;
  }

  /**
   * @return Excluded topics.
   */
  public Set<String> excludedTopics() {
    return Collections.unmodifiableSet(_excludedTopics);
  }

  /**
   * @return Excluded brokers for leadership transfer.
   */
  public Set<Integer> excludedBrokersForLeadership() {
    return Collections.unmodifiableSet(_excludedBrokersForLeadership);
  }

  /**
   * @return Excluded brokers for replica moves.
   */
  public Set<Integer> excludedBrokersForReplicaMove() {
    return Collections.unmodifiableSet(_excludedBrokersForReplicaMove);
  }

  /**
   * @return {@code true} if the optimization request was triggered by goal violation, {@code false} otherwise.
   */
  public boolean isTriggeredByGoalViolation() {
    return _isTriggeredByGoalViolation;
  }

  /**
   * @return Requested destination broker ids.
   */
  public Set<Integer> requestedDestinationBrokerIds() {
    return Collections.unmodifiableSet(_requestedDestinationBrokerIds);
  }

  /**
   * @return {@code true} if the optimization will apply only to immigrant replicas, {@code false} otherwise.
   */
  public boolean onlyMoveImmigrantReplicas() {
    return _onlyMoveImmigrantReplicas;
  }

  /**
   * @return {@code true} to compute proposals in fast mode, {@code false} otherwise.
   */
  public boolean fastMode() {
    return _fastMode;
  }

  @Override
  public String toString() {
    return String.format("[excludedTopics=%s,excludedBrokersForLeadership=%s,excludedBrokersForReplicaMove=%s,"
                         + "isTriggeredByGoalViolation=%s,requestedDestinationBrokerIds=%s,onlyMoveImmigrantReplicas=%s,fastMode=%s]",
                         _excludedTopics, _excludedBrokersForLeadership, _excludedBrokersForReplicaMove, _isTriggeredByGoalViolation,
                         _requestedDestinationBrokerIds, _onlyMoveImmigrantReplicas, _fastMode);
  }
}
