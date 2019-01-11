/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;


public class ExecutorState {
  private static final String TRIGGERED_USER_TASK_ID = "triggeredUserTaskId";
  private static final String STATE = "state";
  private static final String RECENTLY_DEMOTED_BROKERS = "recentlyDemotedBrokers";
  private static final String RECENTLY_REMOVED_BROKERS = "recentlyRemovedBrokers";
  private static final String PENDING_LEADERSHIP_MOVEMENT = "pendingLeadershipMovement";
  private static final String NUM_FINISHED_LEADERSHIP_MOVEMENTS = "numFinishedLeadershipMovements";
  private static final String NUM_TOTAL_LEADERSHIP_MOVEMENTS = "numTotalLeadershipMovements";
  private static final String MAXIMUM_CONCURRENT_LEADER_MOVEMENTS = "maximumConcurrentLeaderMovements";
  private static final String NUM_TOTAL_PARTITION_MOVEMENTS = "numTotalPartitionMovements";
  private static final String TOTAL_DATA_TO_MOVE = "totalDataToMove";
  private static final String NUM_FINISHED_PARTITION_MOVEMENTS = "numFinishedPartitionMovements";
  private static final String FINISHED_DATA_MOVEMENT = "finishedDataMovement";
  private static final String ABORTING_PARTITIONS = "abortingPartitions";
  private static final String ABORTED_PARTITIONS = "abortedPartitions";
  private static final String DEAD_PARTITIONS = "deadPartitions";
  private static final String MAXIMUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER = "maximumConcurrentPartitionMovementsPerBroker";
  private static final String IN_PROGRESS_PARTITION_MOVEMENT = "inProgressPartitionMovement";
  private static final String ABORTING_PARTITION_MOVEMENT = "abortingPartitionMovement";
  private static final String ABORTED_PARTITION_MOVEMENT = "abortedPartitionMovement";
  private static final String DEAD_PARTITION_MOVEMENT = "deadPartitionMovement";
  private static final String ERROR = "error";

  public enum State {
    NO_TASK_IN_PROGRESS,
    STARTING_EXECUTION,
    REPLICA_MOVEMENT_TASK_IN_PROGRESS,
    LEADER_MOVEMENT_TASK_IN_PROGRESS,
    STOPPING_EXECUTION
  }

  private final State _state;
  private final Set<ExecutionTask> _pendingPartitionMovements;
  private final Collection<ExecutionTask> _pendingLeadershipMovements;
  private final int _numFinishedPartitionMovements;
  private final int _numFinishedLeadershipMovements;
  private final Set<ExecutionTask> _inProgressPartitionMovements;
  private final Set<ExecutionTask> _abortingPartitionMovements;
  private final Set<ExecutionTask> _abortedPartitionMovements;
  private final Set<ExecutionTask> _deadPartitionMovements;
  private final long _remainingDataToMoveInMB;
  private final long _finishedDataMovementInMB;
  private final int _maximumConcurrentPartitionMovementsPerBroker;
  private final int _maximumConcurrentLeaderMovements;
  private final String _uuid;
  private final Set<Integer> _recentlyDemotedBrokers;
  private final Set<Integer> _recentlyRemovedBrokers;

  private ExecutorState(State state,
                        int numFinishedPartitionMovements,
                        int numFinishedLeadershipMovements,
                        ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                        long finishedDataMovementInMB,
                        int maximumConcurrentPartitionMovementsPerBroker,
                        int maximumConcurrentLeaderMovements,
                        Set<Integer> recentlyDemotedBrokers,
                        Set<Integer> recentlyRemovedBrokers) {
    this(state,
         numFinishedPartitionMovements,
         numFinishedLeadershipMovements,
         executionTasksSummary,
         finishedDataMovementInMB,
         maximumConcurrentPartitionMovementsPerBroker,
         maximumConcurrentLeaderMovements,
         null,
         recentlyDemotedBrokers,
         recentlyRemovedBrokers);
  }

  private ExecutorState(State state,
                        int numFinishedPartitionMovements,
                        int numFinishedLeadershipMovements,
                        ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                        long finishedDataMovementInMB,
                        int maximumConcurrentPartitionMovementsPerBroker,
                        int maximumConcurrentLeaderMovements,
                        String uuid,
                        Set<Integer> recentlyDemotedBrokers,
                        Set<Integer> recentlyRemovedBrokers) {
    _state = state;
    _numFinishedPartitionMovements = numFinishedPartitionMovements;
    _numFinishedLeadershipMovements = numFinishedLeadershipMovements;
    _pendingPartitionMovements = executionTasksSummary.remainingPartitionMovements();
    _pendingLeadershipMovements = executionTasksSummary.remainingLeadershipMovements();
    _inProgressPartitionMovements = executionTasksSummary.inProgressTasks();
    _abortingPartitionMovements = executionTasksSummary.abortingTasks();
    _abortedPartitionMovements = executionTasksSummary.abortedTasks();
    _deadPartitionMovements = executionTasksSummary.deadTasks();
    _remainingDataToMoveInMB = executionTasksSummary.remainingDataToMoveInMB();
    _finishedDataMovementInMB = finishedDataMovementInMB;
    _maximumConcurrentPartitionMovementsPerBroker = maximumConcurrentPartitionMovementsPerBroker;
    _maximumConcurrentLeaderMovements = maximumConcurrentLeaderMovements;
    _uuid = uuid;
    _recentlyDemotedBrokers = recentlyDemotedBrokers;
    _recentlyRemovedBrokers = recentlyRemovedBrokers;
  }

  /**
   * @param recentlyDemotedBrokers Recently demoted broker IDs.
   * @param recentlyRemovedBrokers Recently removed broker IDs.
   * @return Executor state when no task is in progress.
   */
  public static ExecutorState noTaskInProgress(Set<Integer> recentlyDemotedBrokers, Set<Integer> recentlyRemovedBrokers) {
    return new ExecutorState(State.NO_TASK_IN_PROGRESS,
                             0,
                             0,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L,
                             0,
                             0,
                             recentlyDemotedBrokers,
                             recentlyRemovedBrokers);
  }

  /**
   * @param uuid UUID of the current execution.
   * @param recentlyDemotedBrokers Recently demoted broker IDs.
   * @param recentlyRemovedBrokers Recently removed broker IDs.
   * @return Executor state when the execution has started.
   */
  public static ExecutorState executionStarted(String uuid,
                                               Set<Integer> recentlyDemotedBrokers,
                                               Set<Integer> recentlyRemovedBrokers) {
    return new ExecutorState(State.STARTING_EXECUTION,
                             0,
                             0,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L,
                             0,
                             0,
                             uuid,
                             recentlyDemotedBrokers,
                             recentlyRemovedBrokers);
  }

  /**
   * @param finishedPartitionMovements Finished partition movements.
   * @param executionTasksSummary Summary of the execution tasks.
   * @param finishedDataMovementInMB Completed data movement in megabytes.
   * @param maximumConcurrentPartitionMovementsPerBroker Maximum concurrent partition movements per broker.
   * @param maximumConcurrentLeaderMovements Maximum concurrent leader movements.
   * @param uuid UUID of the current execution.
   * @param recentlyDemotedBrokers Recently demoted broker IDs.
   * @param recentlyRemovedBrokers Recently removed broker IDs.
   * @return Executor state when replica movement is in progress.
   */
  public static ExecutorState replicaMovementInProgress(int finishedPartitionMovements,
                                                        ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                                                        long finishedDataMovementInMB,
                                                        int maximumConcurrentPartitionMovementsPerBroker,
                                                        int maximumConcurrentLeaderMovements,
                                                        String uuid,
                                                        Set<Integer> recentlyDemotedBrokers,
                                                        Set<Integer> recentlyRemovedBrokers) {
    return new ExecutorState(State.REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                             finishedPartitionMovements, 0, executionTasksSummary,
                             finishedDataMovementInMB, maximumConcurrentPartitionMovementsPerBroker,
                             maximumConcurrentLeaderMovements, uuid, recentlyDemotedBrokers, recentlyRemovedBrokers);
  }

  /**
   * @param finishedPartitionMovements Finished partition movements.
   * @param remainingPartitionMovements Remaining partition movements.
   * @param inProgressTasks In progress tasks.
   * @param abortingTasks Aborting tasks.
   * @param abortedTasks Aborted tasks.
   * @param deadTasks Dead tasks.
   * @param remainingDataToMoveInMB Remaining data movement in megabytes.
   * @param finishedDataMovementInMB Completed data movement in megabytes.
   * @param maximumConcurrentPartitionMovementsPerBroker Maximum concurrent partition movements per broker.
   * @param maximumConcurrentLeaderMovements Maximum concurrent leader movements.
   * @param uuid UUID of the current execution.
   * @param recentlyDemotedBrokers Recently demoted broker IDs.
   * @param recentlyRemovedBrokers Recently removed broker IDs.
   * @return Executor state when replica movement is in progress.
   */
  public static ExecutorState replicaMovementInProgress(int finishedPartitionMovements,
                                                        Set<ExecutionTask> remainingPartitionMovements,
                                                        Set<ExecutionTask> inProgressTasks,
                                                        Set<ExecutionTask> abortingTasks,
                                                        Set<ExecutionTask> abortedTasks,
                                                        Set<ExecutionTask> deadTasks,
                                                        long remainingDataToMoveInMB,
                                                        long finishedDataMovementInMB,
                                                        int maximumConcurrentPartitionMovementsPerBroker,
                                                        int maximumConcurrentLeaderMovements,
                                                        String uuid,
                                                        Set<Integer> recentlyDemotedBrokers,
                                                        Set<Integer> recentlyRemovedBrokers) {
    return replicaMovementInProgress(finishedPartitionMovements,
                                     new ExecutionTaskManager.ExecutionTasksSummary(remainingPartitionMovements,
                                                                                    Collections.emptySet(),
                                                                                    inProgressTasks,
                                                                                    abortingTasks,
                                                                                    abortedTasks,
                                                                                    deadTasks,
                                                                                    remainingDataToMoveInMB),
                                     finishedDataMovementInMB,
                                     maximumConcurrentPartitionMovementsPerBroker,
                                     maximumConcurrentLeaderMovements,
                                     uuid,
                                     recentlyDemotedBrokers,
                                     recentlyRemovedBrokers);
  }

  /**
   * @param finishedLeadershipMovements Finished leadership movements.
   * @param executionTasksSummary Summary of the execution tasks.
   * @param maximumConcurrentPartitionMovementsPerBroker Maximum concurrent partition movements per broker.
   * @param maximumConcurrentLeaderMovements Maximum concurrent leader movements.
   * @param uuid UUID of the current execution.
   * @param recentlyDemotedBrokers Recently demoted broker IDs.
   * @param recentlyRemovedBrokers Recently removed broker IDs.
   * @return Executor state when leader movement is in progress.
   */
  public static ExecutorState leaderMovementInProgress(int finishedLeadershipMovements,
                                                       ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                                                       int maximumConcurrentPartitionMovementsPerBroker,
                                                       int maximumConcurrentLeaderMovements,
                                                       String uuid,
                                                       Set<Integer> recentlyDemotedBrokers,
                                                       Set<Integer> recentlyRemovedBrokers) {
    return new ExecutorState(State.LEADER_MOVEMENT_TASK_IN_PROGRESS,
                             0,
                             finishedLeadershipMovements,
                             new ExecutionTaskManager.ExecutionTasksSummary(Collections.emptySet(),
                                                                            executionTasksSummary.remainingLeadershipMovements(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptySet(),
                                                                            0L),
                             0L,
                             maximumConcurrentPartitionMovementsPerBroker,
                             maximumConcurrentLeaderMovements,
                             uuid,
                             recentlyDemotedBrokers,
                             recentlyRemovedBrokers);
  }

  /**
   * @param finishedPartitionMovements Finished partition movements.
   * @param finishedLeadershipMovements Finished leadership movements.
   * @param executionTasksSummary Summary of the execution tasks.
   * @param finishedDataMovementInMB Completed data movement in megabytes.
   * @param maximumConcurrentPartitionMovementsPerBroker Maximum concurrent partition movements per broker.
   * @param maximumConcurrentLeaderMovements Maximum concurrent leader movements.
   * @param uuid UUID of the current execution.
   * @param recentlyDemotedBrokers Recently demoted broker IDs.
   * @param recentlyRemovedBrokers Recently removed broker IDs.
   * @return Executor state when stopping the ongoing execution.
   */
  public static ExecutorState stopping(int finishedPartitionMovements,
                                       int finishedLeadershipMovements,
                                       ExecutionTaskManager.ExecutionTasksSummary executionTasksSummary,
                                       long finishedDataMovementInMB,
                                       int maximumConcurrentPartitionMovementsPerBroker,
                                       int maximumConcurrentLeaderMovements,
                                       String uuid,
                                       Set<Integer> recentlyDemotedBrokers,
                                       Set<Integer> recentlyRemovedBrokers) {
    return new ExecutorState(State.STOPPING_EXECUTION,
                             finishedPartitionMovements,
                             finishedLeadershipMovements,
                             executionTasksSummary,
                             finishedDataMovementInMB,
                             maximumConcurrentPartitionMovementsPerBroker,
                             maximumConcurrentLeaderMovements,
                             uuid,
                             recentlyDemotedBrokers,
                             recentlyRemovedBrokers);
  }

  public State state() {
    return _state;
  }

  public int numTotalPartitionMovements() {
    return _pendingPartitionMovements.size()
           + _inProgressPartitionMovements.size()
           + _numFinishedPartitionMovements
           + _abortingPartitionMovements.size()
           + _abortedPartitionMovements.size();
  }

  public int numTotalLeadershipMovements() {
    return _numFinishedLeadershipMovements + _pendingLeadershipMovements.size();
  }

  public long totalDataToMoveInMB() {
    return _remainingDataToMoveInMB + _finishedDataMovementInMB;
  }

  public int numFinishedPartitionMovements() {
    return _numFinishedPartitionMovements;
  }

  public int numFinishedLeadershipMovements() {
    return _numFinishedLeadershipMovements;
  }

  public Set<ExecutionTask> pendingPartitionMovements() {
    return _pendingPartitionMovements;
  }

  public Collection<ExecutionTask> pendingLeadershipMovements() {
    return _pendingLeadershipMovements;
  }

  public Set<ExecutionTask> inProgressPartitionMovements() {
    return _inProgressPartitionMovements;
  }

  public Set<ExecutionTask> abortingPartitionMovements() {
    return _abortingPartitionMovements;
  }

  public Set<ExecutionTask> abortedPartitionMovements() {
    return _abortedPartitionMovements;
  }

  public Set<ExecutionTask> deadPartitionMovements() {
    return _deadPartitionMovements;
  }

  public String uuid() {
    return _uuid;
  }

  public Set<Integer> recentlyDemotedBrokers() {
    return _recentlyDemotedBrokers;
  }

  public Set<Integer> recentlyRemovedBrokers() {
    return _recentlyRemovedBrokers;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
    Map<String, Object> execState = new HashMap<>();
    execState.put(STATE, _state);
    if (_recentlyDemotedBrokers != null && !_recentlyDemotedBrokers.isEmpty()) {
      execState.put(RECENTLY_DEMOTED_BROKERS, _recentlyDemotedBrokers);
    }
    if (_recentlyRemovedBrokers != null && !_recentlyRemovedBrokers.isEmpty()) {
      execState.put(RECENTLY_REMOVED_BROKERS, _recentlyRemovedBrokers);
    }
    switch (_state) {
      case NO_TASK_IN_PROGRESS:
        break;
      case STARTING_EXECUTION:
        execState.put(TRIGGERED_USER_TASK_ID, _uuid == null ? "Initiated-by-AnomalyDetector" : _uuid);
        break;
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        if (verbose) {
          List<Object> pendingLeadershipMovementList = new ArrayList<>();
          for (ExecutionTask task : _pendingLeadershipMovements) {
            pendingLeadershipMovementList.add(task.getJsonStructure());
          }
          execState.put(PENDING_LEADERSHIP_MOVEMENT, pendingLeadershipMovementList);
        }
        execState.put(NUM_FINISHED_LEADERSHIP_MOVEMENTS, _numFinishedLeadershipMovements);
        execState.put(NUM_TOTAL_LEADERSHIP_MOVEMENTS, numTotalLeadershipMovements());
        execState.put(MAXIMUM_CONCURRENT_LEADER_MOVEMENTS, _maximumConcurrentLeaderMovements);
        execState.put(TRIGGERED_USER_TASK_ID, _uuid == null ? "Initiated-by-AnomalyDetector" : _uuid);
        break;
      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
      case STOPPING_EXECUTION:
        execState.put(NUM_TOTAL_PARTITION_MOVEMENTS, numTotalPartitionMovements());
        execState.put(NUM_TOTAL_LEADERSHIP_MOVEMENTS, numTotalLeadershipMovements());
        execState.put(TOTAL_DATA_TO_MOVE, totalDataToMoveInMB());
        execState.put(NUM_FINISHED_PARTITION_MOVEMENTS, _numFinishedPartitionMovements);
        execState.put(NUM_FINISHED_LEADERSHIP_MOVEMENTS, _numFinishedLeadershipMovements);
        execState.put(FINISHED_DATA_MOVEMENT, _finishedDataMovementInMB);
        execState.put(ABORTING_PARTITIONS, _abortingPartitionMovements);
        execState.put(ABORTED_PARTITIONS, _abortedPartitionMovements);
        execState.put(DEAD_PARTITIONS, _deadPartitionMovements);
        execState.put(MAXIMUM_CONCURRENT_LEADER_MOVEMENTS, _maximumConcurrentLeaderMovements);
        execState.put(MAXIMUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER, _maximumConcurrentPartitionMovementsPerBroker);
        execState.put(TRIGGERED_USER_TASK_ID, _uuid == null ? "Initiated-by-AnomalyDetector" : _uuid);
        if (verbose) {
          List<Object> inProgressPartitionMovementList = new ArrayList<>(_inProgressPartitionMovements.size());
          List<Object> abortingPartitionMovementList = new ArrayList<>(_abortingPartitionMovements.size());
          List<Object> abortedPartitionMovementList = new ArrayList<>(_abortedPartitionMovements.size());
          List<Object> deadPartitionMovementList = new ArrayList<>(_deadPartitionMovements.size());
          List<Object> pendingPartitionMovementList = new ArrayList<>(_pendingPartitionMovements.size());
          for (ExecutionTask task : _inProgressPartitionMovements) {
            inProgressPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : _abortingPartitionMovements) {
            abortingPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : _abortedPartitionMovements) {
            abortedPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : _deadPartitionMovements) {
            deadPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : _pendingPartitionMovements) {
            pendingPartitionMovementList.add(task.getJsonStructure());
          }
          execState.put(IN_PROGRESS_PARTITION_MOVEMENT, inProgressPartitionMovementList);
          execState.put(ABORTING_PARTITION_MOVEMENT, abortingPartitionMovementList);
          execState.put(ABORTED_PARTITION_MOVEMENT, abortedPartitionMovementList);
          execState.put(DEAD_PARTITION_MOVEMENT, deadPartitionMovementList);
          execState.put(_state == State.STOPPING_EXECUTION ? "CancelledPartitionMovement" : "PendingPartitionMovement",
              pendingPartitionMovementList);
        }
        break;
      default:
        execState.put(ERROR, "ILLEGAL_STATE_EXCEPTION");
        execState.remove(RECENTLY_DEMOTED_BROKERS);
        execState.remove(RECENTLY_REMOVED_BROKERS);
        break;
    }

    return execState;
  }

  public String getPlaintext() {
    String recentlyDemotedBrokers = (_recentlyDemotedBrokers != null && !_recentlyDemotedBrokers.isEmpty())
                                    ? String.format(", %s: %s", RECENTLY_DEMOTED_BROKERS, _recentlyDemotedBrokers) : "";
    String recentlyRemovedBrokers = (_recentlyRemovedBrokers != null && !_recentlyRemovedBrokers.isEmpty())
                                    ? String.format(", %s: %s", RECENTLY_REMOVED_BROKERS, _recentlyRemovedBrokers) : "";

    switch (_state) {
      case NO_TASK_IN_PROGRESS:
        return String.format("{%s: %s%s%s}", STATE, _state, recentlyDemotedBrokers, recentlyRemovedBrokers);
      case STARTING_EXECUTION:
        return String.format("{%s: %s, %s: %s%s%s}", STATE, _state, TRIGGERED_USER_TASK_ID,
                             _uuid == null ? "Initiated-by-AnomalyDetector" : _uuid, recentlyDemotedBrokers, recentlyRemovedBrokers);
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        return String.format("{%s: %s, finished/total leadership movements: %d/%d, "
                             + "maximum concurrent leadership movements: %d, %s: %s%s%s}", STATE, _state, _numFinishedLeadershipMovements,
                             numTotalLeadershipMovements(), _maximumConcurrentLeaderMovements, TRIGGERED_USER_TASK_ID,
                             _uuid == null ? "Initiated-by-AnomalyDetector" : _uuid, recentlyDemotedBrokers, recentlyRemovedBrokers);
      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
      case STOPPING_EXECUTION:
        return String.format("{%s: %s, in-progress/aborting partitions: %d/%d, completed/total bytes(MB): %d/%d, "
                             + "finished/aborted/dead/total partitions: %d/%d/%d/%d, finished leadership movements: %d/%d, "
                             + "maximum concurrent leadership/per-broker partition movements: %d/%d, %s: %s%s%s}",
                             STATE, _state, _inProgressPartitionMovements.size(), _abortingPartitionMovements.size(),
                             _finishedDataMovementInMB, totalDataToMoveInMB(), _numFinishedPartitionMovements,
                             _abortedPartitionMovements.size(), _deadPartitionMovements.size(),
                             numTotalPartitionMovements(), _numFinishedLeadershipMovements, numTotalLeadershipMovements(),
                             _maximumConcurrentLeaderMovements, _maximumConcurrentPartitionMovementsPerBroker,
                             TRIGGERED_USER_TASK_ID, _uuid == null ? "Initiated-by-AnomalyDetector" : _uuid,
                             recentlyDemotedBrokers, recentlyRemovedBrokers);
      default:
        throw new IllegalStateException("This should never happen");
    }
  }
}
