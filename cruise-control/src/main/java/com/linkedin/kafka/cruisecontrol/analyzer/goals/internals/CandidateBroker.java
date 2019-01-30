/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.internals;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Set;
import java.util.SortedSet;

import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.utilizationPercentage;


/**
 * A helper class for goals to keep track of the candidate brokers, its sorted replicas with respect to the given
 * resource, and if they are excluded -- e.g. from leadership movement.
 */
public class CandidateBroker implements Comparable<CandidateBroker> {
  private final Broker _broker;
  private final SortedSet<Replica> _replicas;
  private final boolean _isAscending;
  private final boolean _excludedForLeadership;
  private final boolean _excludedForReplicaMove;
  private final Resource _resource;

  public CandidateBroker(Broker broker,
                         Resource resource,
                         SortedSet<Replica> replicas,
                         boolean isAscending,
                         Set<Integer> excludedBrokersForLeadership,
                         Set<Integer> excludedBrokersForReplicaMove) {
    _broker = broker;
    _replicas = replicas;
    _isAscending = isAscending;
    _excludedForLeadership = excludedBrokersForLeadership.contains(_broker.id());
    _excludedForReplicaMove = excludedBrokersForReplicaMove.contains(_broker.id());
    _resource = resource;
  }

  public Broker broker() {
    return _broker;
  }

  public SortedSet<Replica> replicas() {
    return _replicas;
  }

  public Resource resource() {
    return _resource;
  }

  /**
   * Check whether moving the given replica violates the leadership exclusion requirement
   *
   * @param replicaToReceive Candidate replica to move to this broker.
   * @return True if moving the replica would violate the leadership exclusion requirement, false otherwise.
   */
  public boolean shouldExcludeForLeadership(Replica replicaToReceive) {
    return _excludedForLeadership && replicaToReceive.originalBroker().isAlive() && replicaToReceive.isLeader();
  }

  /**
   * Check whether moving the given replica violates the replica move exclusion requirement
   *
   * @param replicaToReceive Candidate replica to move to this broker.
   * @return True if moving the replica would violate the replica move requirement, false otherwise.
   */
  public boolean shouldExcludeForReplicaMove(Replica replicaToReceive) {
    return _excludedForReplicaMove && replicaToReceive.originalBroker().isAlive();
  }

  @Override
  public int compareTo(CandidateBroker o) {
    int result = _isAscending
                 ? Double.compare(utilizationPercentage(_broker, _resource), utilizationPercentage(o._broker, _resource))
                 : Double.compare(utilizationPercentage(o._broker, _resource), utilizationPercentage(_broker, _resource));
    return result != 0 ? result : Integer.compare(_broker.id(), o._broker.id());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CandidateBroker that = (CandidateBroker) o;
    int result = Double.compare(utilizationPercentage(that._broker, _resource), utilizationPercentage(_broker, _resource));
    return result == 0 && _broker.id() == that._broker.id();
  }

  @Override
  public int hashCode() {
    return _broker.id();
  }

  @Override
  public String toString() {
    return String.format("CandidateBroker{%s util: %f, resource: %s, excluded for (leadership: %s, replica move: %s)}",
                         _broker, utilizationPercentage(_broker, _resource),
                         _resource, _excludedForLeadership, _excludedForReplicaMove);
  }
}
