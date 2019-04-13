/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * <p>
 * A class used by the brokers/disks to host the replicas sorted in a certain order.
 * </p>
 *
 *  The SortedReplicas uses three functions to sort the replicas in the broker/disk.
 *  <ul>
 *     <li>
 *      <tt>ScoreFunction</tt>: the score function generates a score for each replica to sort. The replicas are
 *      sorted based on their score in ascending order. Those who want a descending order need to use
 *      the descending iterator of {@link #sortedReplicas()}. As alternatives, {@link #reverselySortedReplicas()}
 *      are provided for convenience.
 *    </li>
 *    <li>
 *      <tt>SelectionFunction</tt>(optional): the selection function decides which replicas to include in the sorted
 *      replica list. For example, in some cases, the users may only want to have sorted leader replicas.
 *    </li>
 *    <li>
 *      <tt>PriorityFunction</tt>(optional): the priority function allows users to prioritize certain replicas in the
 *      sorted replicas. The replicas will be sorted by their priority first. The replicas with the same priority are
 *      then sorted with their score from the <tt>scoreFunction</tt>.
 *      Note that if a priority function is provided, the <tt>NavigableSet</tt> returned by the
 *      {@link #sortedReplicas()} is no longer binary searchable based on the score.
 *    </li>
 *  </ul>
 *
 * <p>
 *   The SortedReplicas are initialized lazily, i.e. until one of {@link #sortedReplicas()},
 *   {@link #reverselySortedReplicas()} and {@link #sortedReplicaWrappers()} is invoked, the sorted replicas
 *   will not be populated.
 * </p>
 */
public class SortedReplicas {
  private final Broker _broker;
  private final Disk _disk;
  private final Map<Replica, ReplicaWrapper> _replicaWrapperMap;
  private final NavigableSet<ReplicaWrapper> _sortedReplicas;
  private final Function<Replica, Boolean> _selectionFunc;
  private final Function<Replica, Integer> _priorityFunc;
  private final Function<Replica, Double> _scoreFunc;
  private boolean _initialized;

  SortedReplicas(Broker broker,
                 Function<Replica, Boolean> selectionFunc,
                 Function<Replica, Integer> priorityFunction,
                 Function<Replica, Double> scoreFunction) {
    this(broker, null, selectionFunc, priorityFunction, scoreFunction, true);
  }

  SortedReplicas(Broker broker,
                 Disk disk,
                 Function<Replica, Boolean> selectionFunc,
                 Function<Replica, Integer> priorityFunc,
                 Function<Replica, Double> scoreFunc,
                 boolean initialize) {
    _broker = broker;
    _disk = disk;
    _sortedReplicas = new TreeSet<>();
    _replicaWrapperMap = new HashMap<>();
    _selectionFunc = selectionFunc;
    _scoreFunc = scoreFunc;
    _priorityFunc = priorityFunc;
    // If the sorted replicas need to be initialized, we set the initialized to false and initialize the replicas
    // lazily. If the sorted replicas do not need to be initialized, we simply set the initialized to true, so that
    // all the methods will function normally.
    _initialized = !initialize;
  }

  /**
   * Get the sorted replica wrappers in the ascending order of their priority and score.
   * This method initialize the sorted replicas if it hasn't been initialized.
   * This method is package accessible for testing.
   *
   * @return the sorted replicas wrappers in the ascending order of their priority and score.
   */
  public NavigableSet<ReplicaWrapper> sortedReplicaWrappers() {
    ensureInitialize();
    return Collections.unmodifiableNavigableSet(_sortedReplicas);
  }

  /**
   * Get the sorted replicas in the ascending order of their priority and score.
   * This method initialize the sorted replicas if it hasn't been initialized.
   *
   * @return the sorted replicas in the ascending order of their priority and score.
   */
  public List<Replica> sortedReplicas() {
    ensureInitialize();
    List<Replica> result = new ArrayList<>(_sortedReplicas.size());
    _sortedReplicas.forEach(rw -> result.add(rw.replica()));
    return result;
  }

  /**
   * Get a list of replicas in the descending order of their priority and score.
   * This method initialize the sorted replicas if it hasn't been initialized.
   *
   * @return a list of replicas in the descending order of their priority and score.
   */
  public List<Replica> reverselySortedReplicas() {
    ensureInitialize();
    List<Replica> result = new ArrayList<>(_sortedReplicas.size());
    Iterator<ReplicaWrapper> reverseIter = _sortedReplicas.descendingIterator();
    while (reverseIter.hasNext()) {
      result.add(reverseIter.next().replica());
    }
    return result;
  }

  /**
   * @return the selection function of this {@link SortedReplicas}
   */
  public Function<Replica, Boolean> selectionFunction() {
    return _selectionFunc;
  }

  /**
   * @return the priority function of this {@link SortedReplicas}
   */
  public Function<Replica, Integer> priorityFunction() {
    return _priorityFunc;
  }

  /**
   * @return the score function of this {@link SortedReplicas}
   */
  public Function<Replica, Double> scoreFunction() {
    return _scoreFunc;
  }

  /**
   * Add a new replicas to the sorted replicas. It has no impact if this {@link SortedReplicas} has not been
   * initialized.
   *
   * @param replica the replica to add.
   */
  public void add(Replica replica) {
    if (_initialized) {
      if (_selectionFunc == null || _selectionFunc.apply(replica)) {
        double score = _scoreFunc.apply(replica);
        ReplicaWrapper rw = new ReplicaWrapper(replica, score, _priorityFunc);
        add(rw);
      }
    }
  }

  /**
   * Remove a new replicas to the sorted replicas. It has no impact if this {@link SortedReplicas} has not been
   * initialized.
   *
   * @param replica the replica to remove.
   */
  void remove(Replica replica) {
    if (_initialized) {
      ReplicaWrapper rw = _replicaWrapperMap.remove(replica);
      if (rw != null) {
        _sortedReplicas.remove(rw);
      }
    }
  }

  // Unit test only function.
  int numReplicas() {
    return _sortedReplicas.size();
  }

  private void ensureInitialize() {
    if (!_initialized) {
      _initialized = true;
      if (_disk != null) {
        _disk.replicas().forEach(this::add);
      } else {
        _broker.replicas().forEach(this::add);
      }
    }
  }

  private void add(ReplicaWrapper rw) {
    ReplicaWrapper old = _replicaWrapperMap.put(rw.replica(), rw);
    if (old != null) {
      _sortedReplicas.remove(old);
    }
    _sortedReplicas.add(rw);
  }
}
