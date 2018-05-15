/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.internals;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Comparator;
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
