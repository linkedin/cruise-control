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

  /**
   * Default value for {@link #_excludedBrokersForLeadership} is an empty set.
   */
  public OptimizationOptions(Set<String> excludedTopics) {
    this(excludedTopics, Collections.emptySet());
  }

  public OptimizationOptions(Set<String> excludedTopics, Set<Integer> excludedBrokersForLeadership) {
    this(excludedTopics, excludedBrokersForLeadership, Collections.emptySet());
  }

  public OptimizationOptions(Set<String> excludedTopics,
                             Set<Integer> excludedBrokersForLeadership,
                             Set<Integer> excludedBrokersForReplicaMove) {
    _excludedTopics = excludedTopics;
    _excludedBrokersForLeadership = excludedBrokersForLeadership;
    _excludedBrokersForReplicaMove = excludedBrokersForReplicaMove;
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

  @Override
  public String toString() {
    return String.format("{excludedTopics: %s, excludedBrokersForLeadership: %s, excludedBrokersForReplicaMove: %s}",
                         _excludedTopics, _excludedBrokersForLeadership, _excludedBrokersForReplicaMove);
  }
}
