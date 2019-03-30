/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;

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
  // Execution task statistics to report.
  private ExecutionTaskTracker.ExecutionTasksSummary _executionTasksSummary;
  // Configs to report.
  private final int _maximumConcurrentPartitionMovementsPerBroker;
  private final int _maximumConcurrentLeaderMovements;
  private final String _uuid;
  private final Set<Integer> _recentlyDemotedBrokers;
  private final Set<Integer> _recentlyRemovedBrokers;

  private ExecutorState(State state,
                        ExecutionTasksSummary executionTasksSummary,
                        int maximumConcurrentPartitionMovementsPerBroker,
                        int maximumConcurrentLeaderMovements,
                        String uuid,
                        Set<Integer> recentlyDemotedBrokers,
                        Set<Integer> recentlyRemovedBrokers) {
    _state = state;
    _executionTasksSummary = executionTasksSummary;
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
  public static ExecutorState noTaskInProgress(Set<Integer> recentlyDemotedBrokers,
                                               Set<Integer> recentlyRemovedBrokers) {
    return new ExecutorState(State.NO_TASK_IN_PROGRESS,
                             null,
                             0,
                             0,
                             "",
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
                             null,
                             0,
                             0,
                             uuid,
                             recentlyDemotedBrokers,
                             recentlyRemovedBrokers);
  }

  /**
   * @param state State of executor.
   * @param maximumConcurrentPartitionMovementsPerBroker Maximum concurrent partition movements per broker.
   * @param maximumConcurrentLeaderMovements Maximum concurrent leader movements.
   * @param uuid UUID of the current execution.
   * @param recentlyDemotedBrokers Recently demoted broker IDs.
   * @param recentlyRemovedBrokers Recently removed broker IDs.
   * @return Executor state when execution is in progress.
   */
  public static ExecutorState operationInProgress(State state,
                                                  ExecutionTasksSummary executionTasksSummary,
                                                  int maximumConcurrentPartitionMovementsPerBroker,
                                                  int maximumConcurrentLeaderMovements,
                                                  String uuid,
                                                  Set<Integer> recentlyDemotedBrokers,
                                                  Set<Integer> recentlyRemovedBrokers) {
    if (state == State.NO_TASK_IN_PROGRESS || state == State.STARTING_EXECUTION) {
      throw new IllegalArgumentException(String.format("%s in not an operation-in-progress executor state.", state));
    }
    return new ExecutorState(state,
                             executionTasksSummary,
                             maximumConcurrentPartitionMovementsPerBroker,
                             maximumConcurrentLeaderMovements,
                             uuid,
                             recentlyDemotedBrokers,
                             recentlyRemovedBrokers);
  }

  public State state() {
    return _state;
  }

  public int numTotalMovements(ExecutionTask.TaskType type) {
    return _executionTasksSummary.taskStat().get(type).values().stream().mapToInt(i -> i).sum();
  }

  public int numFinishedMovements(ExecutionTask.TaskType type) {
    return _executionTasksSummary.taskStat().get(type).get(ExecutionTask.State.DEAD) +
           _executionTasksSummary.taskStat().get(type).get(ExecutionTask.State.COMPLETED) +
           _executionTasksSummary.taskStat().get(type).get(ExecutionTask.State.ABORTED);
  }
  public long totalDataToMoveInMB() {
    return _executionTasksSummary.inExecutionDataMovementInMB() +
           _executionTasksSummary.finishedDataMovementInMB() +
           _executionTasksSummary.remainingDataToMoveInMB();
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

  public ExecutionTasksSummary  executionTasksSummary() {
    return _executionTasksSummary;
  }

  /*
   * Return an object that can be further used to encode into JSON
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
        execState.put(TRIGGERED_USER_TASK_ID, _uuid);
        break;
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        if (verbose) {
          List<Object> pendingLeadershipMovementList = new ArrayList<>();
          for (ExecutionTask task : _executionTasksSummary.taskSnapshot().get(LEADER_ACTION).get(PENDING)) {
            pendingLeadershipMovementList.add(task.getJsonStructure());
          }
          execState.put(PENDING_LEADERSHIP_MOVEMENT, pendingLeadershipMovementList);
        }
        execState.put(NUM_FINISHED_LEADERSHIP_MOVEMENTS, numFinishedMovements(LEADER_ACTION));
        execState.put(NUM_TOTAL_LEADERSHIP_MOVEMENTS, numTotalMovements(LEADER_ACTION));
        execState.put(MAXIMUM_CONCURRENT_LEADER_MOVEMENTS, _maximumConcurrentLeaderMovements);
        execState.put(TRIGGERED_USER_TASK_ID, _uuid);
        break;
      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
      case STOPPING_EXECUTION:
        execState.put(NUM_TOTAL_PARTITION_MOVEMENTS, numTotalMovements(REPLICA_ACTION));
        execState.put(NUM_TOTAL_LEADERSHIP_MOVEMENTS, numTotalMovements(LEADER_ACTION));
        execState.put(TOTAL_DATA_TO_MOVE, totalDataToMoveInMB());
        execState.put(NUM_FINISHED_PARTITION_MOVEMENTS, numFinishedMovements(REPLICA_ACTION));
        execState.put(NUM_FINISHED_LEADERSHIP_MOVEMENTS, numFinishedMovements(LEADER_ACTION));
        execState.put(FINISHED_DATA_MOVEMENT, _executionTasksSummary.finishedDataMovementInMB());
        execState.put(ABORTING_PARTITIONS, _executionTasksSummary.taskStat().get(REPLICA_ACTION).get(ABORTING));
        execState.put(ABORTED_PARTITIONS, _executionTasksSummary.taskStat().get(REPLICA_ACTION).get(ABORTED));
        execState.put(DEAD_PARTITIONS, _executionTasksSummary.taskStat().get(REPLICA_ACTION).get(DEAD));
        execState.put(MAXIMUM_CONCURRENT_LEADER_MOVEMENTS, _maximumConcurrentLeaderMovements);
        execState.put(MAXIMUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER, _maximumConcurrentPartitionMovementsPerBroker);
        execState.put(TRIGGERED_USER_TASK_ID, _uuid);
        if (verbose) {
          Map<ExecutionTask.State, Set<ExecutionTask>> partitionMovementsByState = _executionTasksSummary.taskSnapshot().get(REPLICA_ACTION);
          List<Object> inProgressPartitionMovementList = new ArrayList<>(partitionMovementsByState.get(IN_PROGRESS).size());
          List<Object> abortingPartitionMovementList = new ArrayList<>(partitionMovementsByState.get(ABORTING).size());
          List<Object> abortedPartitionMovementList = new ArrayList<>(partitionMovementsByState.get(ABORTED).size());
          List<Object> deadPartitionMovementList = new ArrayList<>(partitionMovementsByState.get(DEAD).size());
          List<Object> pendingPartitionMovementList = new ArrayList<>(partitionMovementsByState.get(PENDING).size());
          for (ExecutionTask task : partitionMovementsByState.get(IN_PROGRESS)) {
            inProgressPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : partitionMovementsByState.get(ABORTING)) {
            abortingPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : partitionMovementsByState.get(ABORTED)) {
            abortedPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : partitionMovementsByState.get(DEAD)) {
            deadPartitionMovementList.add(task.getJsonStructure());
          }
          for (ExecutionTask task : partitionMovementsByState.get(PENDING)) {
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
                             _uuid, recentlyDemotedBrokers, recentlyRemovedBrokers);
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        return String.format("{%s: %s, finished/total leadership movements: %d/%d, "
                             + "maximum concurrent leadership movements: %d, %s: %s%s%s}", STATE, _state, numFinishedMovements(LEADER_ACTION),
                             numTotalMovements(LEADER_ACTION), _maximumConcurrentLeaderMovements, TRIGGERED_USER_TASK_ID,
                             _uuid, recentlyDemotedBrokers, recentlyRemovedBrokers);
      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
      case STOPPING_EXECUTION:
        Map<ExecutionTask.State, Integer> partitionMovementStats = _executionTasksSummary.taskStat().get(REPLICA_ACTION);
        return String.format("{%s: %s, in-progress/aborting partitions: %d/%d, completed/total bytes(MB): %d/%d, "
                             + "finished/aborted/dead/total partitions: %d/%d/%d/%d, finished leadership movements: %d/%d, "
                             + "maximum concurrent leadership/per-broker partition movements: %d/%d, %s: %s%s%s}",
                             STATE, _state, partitionMovementStats.get(IN_PROGRESS), partitionMovementStats.get(ABORTING),
                             _executionTasksSummary.finishedDataMovementInMB(), totalDataToMoveInMB(), numFinishedMovements(REPLICA_ACTION),
                             partitionMovementStats.get(ABORTED), partitionMovementStats.get(DEAD),
                             numTotalMovements(REPLICA_ACTION), numFinishedMovements(LEADER_ACTION), numTotalMovements(LEADER_ACTION),
                             _maximumConcurrentLeaderMovements, _maximumConcurrentPartitionMovementsPerBroker,
                             TRIGGERED_USER_TASK_ID, _uuid, recentlyDemotedBrokers, recentlyRemovedBrokers);
      default:
        throw new IllegalStateException("This should never happen");
    }
  }
}
