/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
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
  private static final String TRIGGERED_SELF_HEALING_TASK_ID = "triggeredSelfHealingTaskId";
  private static final String STATE = "state";
  private static final String RECENTLY_DEMOTED_BROKERS = "recentlyDemotedBrokers";
  private static final String RECENTLY_REMOVED_BROKERS = "recentlyRemovedBrokers";

  private static final String NUM_TOTAL_LEADERSHIP_MOVEMENTS = "numTotalLeadershipMovements";
  private static final String NUM_PENDING_LEADERSHIP_MOVEMENTS = "numPendingLeadershipMovements";
  private static final String NUM_CANCELLED_LEADERSHIP_MOVEMENTS = "numCancelledLeadershipMovements";
  private static final String NUM_FINISHED_LEADERSHIP_MOVEMENTS = "numFinishedLeadershipMovements";
  private static final String PENDING_LEADERSHIP_MOVEMENT = "pendingLeadershipMovement";
  private static final String CANCELLED_LEADERSHIP_MOVEMENT = "cancelledLeadershipMovement";
  private static final String MAXIMUM_CONCURRENT_LEADER_MOVEMENTS = "maximumConcurrentLeaderMovements";

  private static final String NUM_TOTAL_INTER_BROKER_PARTITION_MOVEMENTS = "numTotalPartitionMovements";
  private static final String NUM_PENDING_INTER_BROKER_PARTITION_MOVEMENTS = "numPendingPartitionMovements";
  private static final String NUM_CANCELLED_INTER_BROKER_PARTITION_MOVEMENTS = "numCancelledPartitionMovements";
  private static final String NUM_IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENTS = "numInProgressPartitionMovements";
  private static final String NUM_ABORTING_INTER_BROKER_PARTITION_MOVEMENTS = "abortingPartitions";
  private static final String NUM_FINISHED_INTER_BROKER_PARTITION_MOVEMENTS = "numFinishedPartitionMovements";
  private static final String IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENT = "inProgressPartitionMovement";
  private static final String PENDING_INTER_BROKER_PARTITION_MOVEMENT = "pendingPartitionMovement";
  private static final String CANCELLED_INTER_BROKER_PARTITION_MOVEMENT = "cancelledPartitionMovement";
  private static final String DEAD_INTER_BROKER_PARTITION_MOVEMENT = "deadPartitionMovement";
  private static final String COMPLETED_INTER_BROKER_PARTITION_MOVEMENT = "completedPartitionMovement";
  private static final String ABORTING_INTER_BROKER_PARTITION_MOVEMENT = "abortingPartitionMovement";
  private static final String ABORTED_INTER_BROKER_PARTITION_MOVEMENT = "abortedPartitionMovement";
  private static final String FINISHED_INTER_BROKER_DATA_MOVEMENT = "finishedDataMovement";
  private static final String TOTAL_INTER_BROKER_DATA_TO_MOVE = "totalDataToMove";
  private static final String MAXIMUM_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER = "maximumConcurrentPartitionMovementsPerBroker";

  private static final String NUM_TOTAL_INTRA_BROKER_PARTITION_MOVEMENTS = "numTotalIntraBrokerPartitionMovements";
  private static final String NUM_FINISHED_INTRA_BROKER_PARTITION_MOVEMENTS = "numFinishedIntraBrokerPartitionMovements";
  private static final String NUM_IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENTS = "numInProgressIntraBrokerPartitionMovements";
  private static final String NUM_ABORTING_INTRA_BROKER_PARTITION_MOVEMENTS = "numAbortingIntraBrokerPartitionMovements";
  private static final String NUM_PENDING_INTRA_BROKER_PARTITION_MOVEMENTS = "numPendingIntraBrokerPartitionMovements";
  private static final String NUM_CANCELLED_INTRA_BROKER_PARTITION_MOVEMENTS = "numCancelledIntraBrokerPartitionMovements";
  private static final String IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENT = "inProgressIntraBrokerPartitionMovement";
  private static final String PENDING_INTRA_BROKER_PARTITION_MOVEMENT = "pendingIntraBrokerPartitionMovement";
  private static final String CANCELLED_INTRA_BROKER_PARTITION_MOVEMENT = "cancelledIntraBrokerPartitionMovement";
  private static final String DEAD_INTRA_BROKER_PARTITION_MOVEMENT = "deadIntraBrokerPartitionMovement";
  private static final String COMPLETED_INTRA_BROKER_PARTITION_MOVEMENT = "completedIntraBrokerPartitionMovement";
  private static final String ABORTING_INTRA_BROKER_PARTITION_MOVEMENT = "abortingIntraBrokerPartitionMovement";
  private static final String ABORTED_INTRA_BROKER_PARTITION_MOVEMENT = "abortedIntraBrokerPartitionMovement";
  private static final String FINISHED_INTRA_BROKER_DATA_MOVEMENT = "finishedIntraBrokerDataMovement";
  private static final String TOTAL_INTRA_BROKER_DATA_TO_MOVE = "totalIntraBrokerDataToMove";
  private static final String MAXIMUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER = "maximumConcurrentIntraBrokerPartitionMovementsPerBroker";

  private static final String ERROR = "error";

  public enum State {
    NO_TASK_IN_PROGRESS,
    STARTING_EXECUTION,
    INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
    INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
    LEADER_MOVEMENT_TASK_IN_PROGRESS,
    STOPPING_EXECUTION
  }

  private final State _state;
  // Execution task statistics to report.
  private ExecutionTaskTracker.ExecutionTasksSummary _executionTasksSummary;
  // Configs to report.
  private final int _maximumConcurrentInterBrokerPartitionMovementsPerBroker;
  private final int _maximumConcurrentIntraBrokerPartitionMovementsPerBroker;
  private final int _maximumConcurrentLeaderMovements;
  private final String _uuid;
  private final Set<Integer> _recentlyDemotedBrokers;
  private final Set<Integer> _recentlyRemovedBrokers;

  private ExecutorState(State state,
                        ExecutionTasksSummary executionTasksSummary,
                        int maximumConcurrentInterBrokerPartitionMovementsPerBroker,
                        int maximumConcurrentIntraBrokerPartitionMovementsPerBroker,
                        int maximumConcurrentLeaderMovements,
                        String uuid,
                        Set<Integer> recentlyDemotedBrokers,
                        Set<Integer> recentlyRemovedBrokers) {
    _state = state;
    _executionTasksSummary = executionTasksSummary;
    _maximumConcurrentInterBrokerPartitionMovementsPerBroker = maximumConcurrentInterBrokerPartitionMovementsPerBroker;
    _maximumConcurrentIntraBrokerPartitionMovementsPerBroker = maximumConcurrentIntraBrokerPartitionMovementsPerBroker;
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
                             0,
                             uuid,
                             recentlyDemotedBrokers,
                             recentlyRemovedBrokers);
  }

  /**
   * @param state State of executor.
   * @param executionTasksSummary Summary of the execution tasks.
   * @param maximumConcurrentInterBrokerPartitionMovementsPerBroker Maximum concurrent inter-broker partition movement per broker.
   * @param maximumConcurrentIntraBrokerPartitionMovementsPerBroker Maximum concurrent intra-broker partition movement per broker.
   * @param maximumConcurrentLeaderMovements Maximum concurrent leader movements.
   * @param uuid UUID of the current execution.
   * @param recentlyDemotedBrokers Recently demoted broker IDs.
   * @param recentlyRemovedBrokers Recently removed broker IDs.
   * @return Executor state when execution is in progress.
   */
  public static ExecutorState operationInProgress(State state,
                                                  ExecutionTasksSummary executionTasksSummary,
                                                  int maximumConcurrentInterBrokerPartitionMovementsPerBroker,
                                                  int maximumConcurrentIntraBrokerPartitionMovementsPerBroker,
                                                  int maximumConcurrentLeaderMovements,
                                                  String uuid,
                                                  Set<Integer> recentlyDemotedBrokers,
                                                  Set<Integer> recentlyRemovedBrokers) {
    if (state == State.NO_TASK_IN_PROGRESS || state == State.STARTING_EXECUTION) {
      throw new IllegalArgumentException(String.format("%s is not an operation-in-progress executor state.", state));
    }
    return new ExecutorState(state,
                             executionTasksSummary,
                             maximumConcurrentInterBrokerPartitionMovementsPerBroker,
                             maximumConcurrentIntraBrokerPartitionMovementsPerBroker,
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
    return _executionTasksSummary.taskStat().get(type).get(DEAD) +
           _executionTasksSummary.taskStat().get(type).get(COMPLETED) +
           _executionTasksSummary.taskStat().get(type).get(ABORTED);
  }

  public long numTotalInterBrokerDataToMove() {
    return _executionTasksSummary.inExecutionInterBrokerDataMovementInMB() +
           _executionTasksSummary.finishedInterBrokerDataMovementInMB() +
           _executionTasksSummary.remainingInterBrokerDataToMoveInMB();
  }

  public long numTotalIntraBrokerDataToMove() {
    return _executionTasksSummary.inExecutionIntraBrokerDataMovementInMB() +
           _executionTasksSummary.finishedIntraBrokerDataMovementInMB() +
           _executionTasksSummary.remainingIntraBrokerDataToMoveInMB();
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

  private List<Object> getTaskDetails(ExecutionTask.TaskType type, ExecutionTask.State state) {
    List<Object> taskList = new ArrayList<>();
    for (ExecutionTask task : _executionTasksSummary.filteredTasksByState().get(type).get(state)) {
      taskList.add(task.getJsonStructure());
    }
    return taskList;
  }

  private void populateUuidFieldInJsonStructure(Map<String, Object> execState, String uuid) {
    if (isUuidFromSelfHealing(uuid)) {
      execState.put(TRIGGERED_SELF_HEALING_TASK_ID, uuid);
      execState.put(TRIGGERED_USER_TASK_ID, "");
    } else {
      execState.put(TRIGGERED_SELF_HEALING_TASK_ID, "");
      execState.put(TRIGGERED_USER_TASK_ID, uuid);
    }
  }

  private boolean isUuidFromSelfHealing(String uuid) {
    return AnomalyType.cachedValues().stream().anyMatch(anomalyType -> uuid.startsWith(anomalyType.toString()));
  }

  /**
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
    Map<ExecutionTask.State, Integer> interBrokerPartitionMovementStats;
    Map<ExecutionTask.State, Integer> intraBrokerPartitionMovementStats;
    switch (_state) {
      case NO_TASK_IN_PROGRESS:
        break;
      case STARTING_EXECUTION:
        populateUuidFieldInJsonStructure(execState, _uuid);
        break;
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        populateUuidFieldInJsonStructure(execState, _uuid);
        execState.put(MAXIMUM_CONCURRENT_LEADER_MOVEMENTS, _maximumConcurrentLeaderMovements);
        execState.put(NUM_PENDING_LEADERSHIP_MOVEMENTS, _executionTasksSummary.taskStat().get(LEADER_ACTION).get(PENDING));
        execState.put(NUM_FINISHED_LEADERSHIP_MOVEMENTS, numFinishedMovements(LEADER_ACTION));
        execState.put(NUM_TOTAL_LEADERSHIP_MOVEMENTS, numTotalMovements(LEADER_ACTION));
        if (verbose) {
          execState.put(PENDING_LEADERSHIP_MOVEMENT, getTaskDetails(LEADER_ACTION, PENDING));
        }
        break;
      case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
        interBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
        populateUuidFieldInJsonStructure(execState, _uuid);
        execState.put(MAXIMUM_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER, _maximumConcurrentInterBrokerPartitionMovementsPerBroker);
        execState.put(NUM_IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(IN_PROGRESS));
        execState.put(NUM_ABORTING_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(ABORTING));
        execState.put(NUM_PENDING_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(PENDING));
        execState.put(NUM_FINISHED_INTER_BROKER_PARTITION_MOVEMENTS, numFinishedMovements(INTER_BROKER_REPLICA_ACTION));
        execState.put(NUM_TOTAL_INTER_BROKER_PARTITION_MOVEMENTS, numTotalMovements(INTER_BROKER_REPLICA_ACTION));
        execState.put(FINISHED_INTER_BROKER_DATA_MOVEMENT, _executionTasksSummary.finishedInterBrokerDataMovementInMB());
        execState.put(TOTAL_INTER_BROKER_DATA_TO_MOVE, numTotalInterBrokerDataToMove());
        if (verbose) {
          execState.put(IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, IN_PROGRESS));
          execState.put(PENDING_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, PENDING));
          execState.put(ABORTING_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ABORTING));
          execState.put(ABORTED_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ABORTED));
          execState.put(DEAD_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, DEAD));
          execState.put(COMPLETED_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, COMPLETED));
        }
        break;
      case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
        intraBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
        populateUuidFieldInJsonStructure(execState, _uuid);
        execState.put(MAXIMUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER, _maximumConcurrentIntraBrokerPartitionMovementsPerBroker);
        execState.put(NUM_IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(IN_PROGRESS));
        execState.put(NUM_ABORTING_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(ABORTING));
        execState.put(NUM_PENDING_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(PENDING));
        execState.put(NUM_FINISHED_INTRA_BROKER_PARTITION_MOVEMENTS, numFinishedMovements(INTRA_BROKER_REPLICA_ACTION));
        execState.put(NUM_TOTAL_INTRA_BROKER_PARTITION_MOVEMENTS, numTotalMovements(INTRA_BROKER_REPLICA_ACTION));
        execState.put(FINISHED_INTRA_BROKER_DATA_MOVEMENT, _executionTasksSummary.finishedIntraBrokerDataMovementInMB());
        execState.put(TOTAL_INTRA_BROKER_DATA_TO_MOVE, numTotalIntraBrokerDataToMove());
        if (verbose) {
          execState.put(IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, IN_PROGRESS));
          execState.put(PENDING_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, PENDING));
          execState.put(ABORTING_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ABORTING));
          execState.put(ABORTED_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ABORTED));
          execState.put(DEAD_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, DEAD));
          execState.put(COMPLETED_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, COMPLETED));
        }
        break;
      case STOPPING_EXECUTION:
        interBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
        intraBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
        populateUuidFieldInJsonStructure(execState, _uuid);
        execState.put(MAXIMUM_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER, _maximumConcurrentInterBrokerPartitionMovementsPerBroker);
        execState.put(MAXIMUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER, _maximumConcurrentIntraBrokerPartitionMovementsPerBroker);
        execState.put(MAXIMUM_CONCURRENT_LEADER_MOVEMENTS, _maximumConcurrentLeaderMovements);
        execState.put(NUM_CANCELLED_LEADERSHIP_MOVEMENTS, _executionTasksSummary.taskStat().get(LEADER_ACTION).get(PENDING));
        execState.put(NUM_IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(IN_PROGRESS));
        execState.put(NUM_ABORTING_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(ABORTING));
        execState.put(NUM_CANCELLED_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(PENDING));
        execState.put(NUM_IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(IN_PROGRESS));
        execState.put(NUM_ABORTING_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(ABORTING));
        execState.put(NUM_CANCELLED_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(PENDING));
        if (verbose) {
          execState.put(CANCELLED_LEADERSHIP_MOVEMENT, getTaskDetails(LEADER_ACTION, ExecutionTask.State.PENDING));
          execState.put(CANCELLED_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, PENDING));
          execState.put(IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, IN_PROGRESS));
          execState.put(ABORTING_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ABORTING));
          execState.put(CANCELLED_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, PENDING));
          execState.put(IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, IN_PROGRESS));
          execState.put(ABORTING_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ABORTING));
        }
        break;
      default:
        execState.clear();
        execState.put(ERROR, "ILLEGAL_STATE_EXCEPTION");
        break;
    }
    return execState;
  }

  public String getPlaintext() {
    String recentlyDemotedBrokers = (_recentlyDemotedBrokers != null && !_recentlyDemotedBrokers.isEmpty())
                                    ? String.format(", %s: %s", RECENTLY_DEMOTED_BROKERS, _recentlyDemotedBrokers) : "";
    String recentlyRemovedBrokers = (_recentlyRemovedBrokers != null && !_recentlyRemovedBrokers.isEmpty())
                                    ? String.format(", %s: %s", RECENTLY_REMOVED_BROKERS, _recentlyRemovedBrokers) : "";
    Map<ExecutionTask.State, Integer> interBrokerPartitionMovementStats;
    Map<ExecutionTask.State, Integer> intraBrokerPartitionMovementStats;
    switch (_state) {
      case NO_TASK_IN_PROGRESS:
        return String.format("{%s: %s%s%s}", STATE, _state, recentlyDemotedBrokers, recentlyRemovedBrokers);
      case STARTING_EXECUTION:
        return String.format("{%s: %s, %s: %s%s%s}", STATE, _state,
                             isUuidFromSelfHealing(_uuid) ? TRIGGERED_SELF_HEALING_TASK_ID : TRIGGERED_USER_TASK_ID,
                             _uuid, recentlyDemotedBrokers, recentlyRemovedBrokers);
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        return String.format("{%s: %s, finished/total leadership movements: %d/%d, maximum concurrent leadership movements: %d, %s: %s%s%s}",
                             STATE, _state, numFinishedMovements(LEADER_ACTION), numTotalMovements(LEADER_ACTION),
                             _maximumConcurrentLeaderMovements, isUuidFromSelfHealing(_uuid) ? TRIGGERED_SELF_HEALING_TASK_ID : TRIGGERED_USER_TASK_ID,
                             _uuid, recentlyDemotedBrokers, recentlyRemovedBrokers);
      case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
        interBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
        return String.format("{%s: %s, pending/in-progress/aborting/finished/total inter-broker partition movement %d/%d/%d/%d/%d," +
                             " completed/total bytes(MB): %d/%d, maximum concurrent inter-broker partition movements per-broker: %d, %s: %s%s%s}",
                             STATE, _state,
                             interBrokerPartitionMovementStats.get(PENDING),
                             interBrokerPartitionMovementStats.get(IN_PROGRESS),
                             interBrokerPartitionMovementStats.get(ABORTING),
                             numFinishedMovements(INTER_BROKER_REPLICA_ACTION),
                             numTotalMovements(INTER_BROKER_REPLICA_ACTION),
                             _executionTasksSummary.finishedInterBrokerDataMovementInMB(),
                             numTotalInterBrokerDataToMove(), _maximumConcurrentInterBrokerPartitionMovementsPerBroker,
                             isUuidFromSelfHealing(_uuid) ? TRIGGERED_SELF_HEALING_TASK_ID : TRIGGERED_USER_TASK_ID, _uuid,
                             recentlyDemotedBrokers, recentlyRemovedBrokers);
      case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
        intraBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
        return String.format("{%s: %s, pending/in-progress/aborting/finished/total intra-broker partition movement %d/%d/%d/%d/%d," +
                " completed/total bytes(MB): %d/%d, maximum concurrent intra-broker partition movements per-broker: %d, %s: %s%s%s}",
                STATE, _state,
                intraBrokerPartitionMovementStats.get(PENDING),
                intraBrokerPartitionMovementStats.get(IN_PROGRESS),
                intraBrokerPartitionMovementStats.get(ABORTING),
                numFinishedMovements(INTRA_BROKER_REPLICA_ACTION),
                numTotalMovements(INTRA_BROKER_REPLICA_ACTION),
                _executionTasksSummary.finishedIntraBrokerDataMovementInMB(),
                numTotalIntraBrokerDataToMove(), _maximumConcurrentIntraBrokerPartitionMovementsPerBroker,
                isUuidFromSelfHealing(_uuid) ? TRIGGERED_SELF_HEALING_TASK_ID : TRIGGERED_USER_TASK_ID, _uuid,
                recentlyDemotedBrokers, recentlyRemovedBrokers);
      case STOPPING_EXECUTION:
        interBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
        intraBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
        return String.format("{%s: %s, cancelled/in-progress/aborting/total intra-broker partition movement %d/%d/%d/%d,"
                             + "cancelled/in-progress/aborting/total inter-broker partition movements movements: %d/%d/%d/%d,"
                             + "cancelled/total leadership movements: %d/%d, maximum concurrent intra-broker partition movements per-broker: %d, "
                             + "maximum concurrent inter-broker partition movements per-broker: %d, maximum concurrent leadership movements: %d, "
                             + "%s: %s%s%s}",
                             STATE, _state,
                             intraBrokerPartitionMovementStats.get(PENDING),
                             intraBrokerPartitionMovementStats.get(IN_PROGRESS),
                             intraBrokerPartitionMovementStats.get(ABORTING),
                             numTotalMovements(INTRA_BROKER_REPLICA_ACTION),
                             interBrokerPartitionMovementStats.get(PENDING),
                             interBrokerPartitionMovementStats.get(IN_PROGRESS),
                             interBrokerPartitionMovementStats.get(ABORTING),
                             numTotalMovements(INTER_BROKER_REPLICA_ACTION),
                             _executionTasksSummary.taskStat().get(LEADER_ACTION).get(ExecutionTask.State.PENDING),
                             numTotalMovements(LEADER_ACTION),
                             _maximumConcurrentIntraBrokerPartitionMovementsPerBroker,
                             _maximumConcurrentInterBrokerPartitionMovementsPerBroker,
                             _maximumConcurrentLeaderMovements,
                             isUuidFromSelfHealing(_uuid) ? TRIGGERED_SELF_HEALING_TASK_ID : TRIGGERED_USER_TASK_ID, _uuid,
                             recentlyDemotedBrokers, recentlyRemovedBrokers);
      default:
        throw new IllegalStateException("This should never happen");
    }
  }
}
