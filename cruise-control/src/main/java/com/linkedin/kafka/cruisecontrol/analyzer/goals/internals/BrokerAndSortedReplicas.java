/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.internals;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;


/**
 * A class that maintains the broker and a sorted set of replicas based on a given comparator.
 */
public class BrokerAndSortedReplicas {
  private final Broker _broker;
  private final NavigableSet<Replica> sortedReplicas;

  public BrokerAndSortedReplicas(Broker broker, Comparator<Replica> comparator) {
    _broker = broker;
    sortedReplicas = new TreeSet<>((r1, r2) -> {
      int result = comparator.compare(r1, r2);
      return result == 0 ? r1.compareTo(r2) : result;
    });
    sortedReplicas.addAll(broker.replicas());
  }

  public Broker broker() {
    return _broker;
  }

  public NavigableSet<Replica> sortedReplicas() {
    return sortedReplicas;
  }

  /**
   * Get sorted replicas to move out of this broker, which are filtered based on the state of the cluster.
   *
   * If cluster has dead brokers, but this broker is alive, then replicas to move out are limited to the immigrants.
   *
   * @param clusterModel The state of the cluster.
   * @return sorted replicas to move out of this broker, which are filtered based on the state of the cluster.
   */
  public List<Replica> replicasToMoveOut(ClusterModel clusterModel) {
    // Cluster has offline replicas, but this overloaded broker is alive -- we can move out only the immigrant replicas.
    if (!clusterModel.deadBrokers().isEmpty() && _broker.isAlive()) {
      // Return the sorted immigrant replicas.
      for (Replica replica : sortedReplicas()) {
        if (!_broker.immigrantReplicas().contains(replica)) {
          return new ArrayList<>(sortedReplicas().subSet(sortedReplicas().first(), replica));
        }
      }
    }

    return new ArrayList<>(sortedReplicas());
  }

  @Override
  public int hashCode() {
    return _broker.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof BrokerAndSortedReplicas
        && _broker.equals(((BrokerAndSortedReplicas) obj).broker());
  }
}
