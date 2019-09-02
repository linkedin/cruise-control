/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;


/**
 * A helper class to create new {@link SortedReplicas} for cluster/broker/disk.
 * This class provides fluent API to enable user to customize {@link SortedReplicas} by adding new selection/priority/score
 * functions one by one.
 *
 * One example is as follows( to register a SortedReplica to clusterModel which includes only follower replicas, prioritize offline
 * replicas and sort replicas based on replica'disk utilization).
 * <p>
 * <pre>
 * {@code
 * new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectFollowers())
 *                           .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeOfflineReplicas());
 *                           .addScoreFunc(addScoreFunc(ReplicaSortFunctionFactory.sortByMetricGroupValue(DISK.name())))
 *                           .trackSortedReplicasFor(sortName, clusterModel)
 * } </pre>
 * </p>
 */
public class SortedReplicasHelper {

  private Set<Function<Replica, Boolean>> _selectionFuncs;
  private List<Function<Replica, Integer>> _priorityFuncs;
  private Function<Replica, Double> _scoreFunc;

  public SortedReplicasHelper() {
    _selectionFuncs = new HashSet<>();
    _priorityFuncs = new ArrayList<>();
  }

  /**
   * If condition hold, add a selection function to the {@link SortedReplicas} to be created.
   *
   * @param selectionFunc The selection function to add.
   * @param condition Whether add the selection function or not.
   * @return The helper object itself.
   */
  public SortedReplicasHelper maybeAddSelectionFunc(Function<Replica, Boolean> selectionFunc, boolean condition) {
    if (condition) {
      _selectionFuncs.add(selectionFunc);
    }
    return this;
  }

  /**
   * Add a selection function to the {@link SortedReplicas} to be created.
   *
   * @param selectionFunc The selection function to add.
   * @return The helper object itself.
   */
  public SortedReplicasHelper addSelectionFunc(Function<Replica, Boolean> selectionFunc) {
    return maybeAddSelectionFunc(selectionFunc, true);
  }


  /**
   * If condition hold, add a priority function to the {@link SortedReplicas} to be created.
   * Note the order adding priority function matters. Each call of this method or {@link this#addPriorityFunc(Function)}
   * will add new priority function to the end of priority function list, which will be applied only when all the previous
   * priority functions cannot resolve two replica's priority.
   *
   * @param priorityFunc The priority function to add.
   * @param condition Whether add the priority function or not.
   * @return The helper object itself.
   */
  public SortedReplicasHelper maybeAddPriorityFunc(Function<Replica, Integer> priorityFunc, boolean condition) {
    if (condition && !_priorityFuncs.contains(priorityFunc)) {
      _priorityFuncs.add(priorityFunc);
    }
    return this;
  }

  /**
   * Add a priority function to the {@link SortedReplicas} to be created.
   * Note the order adding priority function matters. Each call of this method or {@link this#maybeAddPriorityFunc(Function, boolean)}
   * will add new priority function to the end of priority function list, which will be applied only when all the previous
   * priority functions cannot resolve two replica's priority.
   *
   * @param priorityFunc The priority function to add.
   * @return The helper object itself.
   */
  public SortedReplicasHelper addPriorityFunc(Function<Replica, Integer> priorityFunc) {
    return maybeAddPriorityFunc(priorityFunc, true);
  }

  /**
   * If condition hold, add a score function to the {@link SortedReplicas} to be created.
   *
   * @param scoreFunc The score function to add.
   * @param condition Whether add the score function or not.
   * @return The helper object itself.
   */
  public SortedReplicasHelper maybeAddScoreFunc(Function<Replica, Double> scoreFunc, boolean condition) {
    if (condition) {
      _scoreFunc = scoreFunc;
    }
    return this;
  }

  /**
   * Add a score function to the {@link SortedReplicas} to be created.
   *
   * @param scoreFunc The score function to add.
   * @return The helper object itself.
   */
  public SortedReplicasHelper addScoreFunc(Function<Replica, Double> scoreFunc) {
    return maybeAddScoreFunc(scoreFunc, true);
  }

  /**
   * Create {@link SortedReplicas} for given {@link Disk} with added selection/priority/score functions.
   *
   * @param sortName The name of {@link SortedReplicas}.
   * @param disk The disk to create {@link SortedReplicas}.
   */
  public void trackSortedReplicasFor(String sortName, Disk disk) {
    disk.trackSortedReplicas(sortName, new HashSet<>(_selectionFuncs), new ArrayList<>(_priorityFuncs), _scoreFunc);
  }

  /**
   * Create {@link SortedReplicas} for given {@link Broker} with added selection/priority/score functions.
   *
   * @param sortName The name of {@link SortedReplicas}.
   * @param broker The broker to create {@link SortedReplicas}.
   */
  public void trackSortedReplicasFor(String sortName, Broker broker) {
    broker.trackSortedReplicas(sortName, new HashSet<>(_selectionFuncs), new ArrayList<>(_priorityFuncs), _scoreFunc);
  }

  /**
   * Create {@link SortedReplicas} for each brokers in the given {@link ClusterModel} with added selection/priority/score functions.
   *
   * @param sortName The name of {@link SortedReplicas}.
   * @param clusterModel The cluster model to create {@link SortedReplicas}.
   */
  public void trackSortedReplicasFor(String sortName, ClusterModel clusterModel) {
    clusterModel.trackSortedReplicas(sortName, new HashSet<>(_selectionFuncs), new ArrayList<>(_priorityFuncs), _scoreFunc);
  }
}