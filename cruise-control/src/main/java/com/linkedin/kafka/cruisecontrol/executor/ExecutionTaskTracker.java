/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.utils.Time;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State;

/**
 * A class for tracking the (1) dead tasks, (2) aborting/aborted tasks, (3) in progress tasks, and (4) pending tasks.
 *
 * This class is not thread-safe.
 */
public class ExecutionTaskTracker {
  private final Map<TaskType, Map<State, Set<ExecutionTask>>> _tasksByType;
  private long _remainingInterBrokerDataToMoveInMB;
  private long _inExecutionInterBrokerDataMovementInMB;
  private long _finishedInterBrokerDataMovementInMB;
  private boolean _isKafkaAssignerMode;
  private final Time _time;

  private static final String INTER_BROKER_REPLICA_ACTION = "replica-action";
  private static final String LEADERSHIP_ACTION = "leadership-action";
  private static final String IN_PROGRESS = "in-progress";
  private static final String PENDING = "pending";
  private static final String ABORTING = "aborting";
  private static final String ABORTED = "aborted";
  private static final String DEAD = "dead";
  private static final String COMPLETED = "completed";
  private static final String GAUGE_ONGOING_EXECUTION_IN_KAFKA_ASSIGNER_MODE = "ongoing-execution-kafka_assigner";
  private static final String GAUGE_ONGOING_EXECUTION_IN_NON_KAFKA_ASSIGNER_MODE = "ongoing-execution-non_kafka_assigner";

  ExecutionTaskTracker(MetricRegistry dropwizardMetricRegistry, Time time) {
    List<State> states = State.cachedValues();
    List<TaskType> taskTypes = TaskType.cachedValues();
    _tasksByType = new HashMap<>(taskTypes.size());
    for (TaskType type : taskTypes) {
      Map<State, Set<ExecutionTask>> taskMap = new HashMap<>(states.size());
      for (State state : states) {
        taskMap.put(state, new HashSet<>());
      }
      _tasksByType.put(type, taskMap);
    }
    _remainingInterBrokerDataToMoveInMB = 0L;
    _inExecutionInterBrokerDataMovementInMB = 0L;
    _finishedInterBrokerDataMovementInMB = 0L;
    _isKafkaAssignerMode = false;
    _time = time;

    // Register gauge sensors.
    registerGaugeSensors(dropwizardMetricRegistry);
  }

  private void registerGaugeSensors(MetricRegistry dropwizardMetricRegistry) {
    String metricName = "Executor";
    for (TaskType type : TaskType.cachedValues()) {
      for (State state : State.cachedValues()) {
        String typeString =  type == TaskType.INTER_BROKER_REPLICA_ACTION ? INTER_BROKER_REPLICA_ACTION :
                                                                            LEADERSHIP_ACTION;
        String stateString =  state == State.PENDING     ? PENDING :
                              state == State.IN_PROGRESS ? IN_PROGRESS :
                              state == State.ABORTING    ? ABORTING :
                              state == State.ABORTED     ? ABORTED :
                              state == State.COMPLETED   ? COMPLETED :
                                                           DEAD;
        dropwizardMetricRegistry.register(MetricRegistry.name(metricName, typeString + "-" + stateString),
                                          (Gauge<Integer>) () -> _tasksByType.get(type).get(state).size());
      }
    }
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_ONGOING_EXECUTION_IN_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) () -> _isKafkaAssignerMode
                                                             && !inExecutionTasks(TaskType.cachedValues()).isEmpty() ? 1 : 0);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_ONGOING_EXECUTION_IN_NON_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) () -> !_isKafkaAssignerMode
                                                             && !inExecutionTasks(TaskType.cachedValues()).isEmpty() ? 1 : 0);
  }

  /**
   * Update the execution state of the task.
   *
   * @param task The task to update.
   * @param newState New execution state of the task.
   */
  public void markTaskState(ExecutionTask task, State newState) {
    _tasksByType.get(task.type()).get(task.state()).remove(task);
    switch (newState) {
      case PENDING:
        break; // Let it go.
      case IN_PROGRESS:
        task.inProgress(_time.milliseconds());
        updateDataMovement(task);
        break;
      case ABORTING:
        task.abort();
        break;
      case ABORTED:
        task.aborted(_time.milliseconds());
        updateDataMovement(task);
        break;
      case COMPLETED:
        task.completed(_time.milliseconds());
        updateDataMovement(task);
        break;
      case DEAD:
        task.kill(_time.milliseconds());
        updateDataMovement(task);
        break;
      default:
        break;
    }
    _tasksByType.get(task.type()).get(newState).add(task);
  }

  private void updateDataMovement(ExecutionTask task) {
    if (task.type() == TaskType.LEADER_ACTION) {
      return;
    }
    long dataToMove = task.proposal().dataToMoveInMB();
    if (task.state() == State.IN_PROGRESS) {
      _remainingInterBrokerDataToMoveInMB -= dataToMove;
      _inExecutionInterBrokerDataMovementInMB += dataToMove;
    } else if (task.state() == State.ABORTED || task.state() == State.DEAD || task.state() == State.COMPLETED) {
      _inExecutionInterBrokerDataMovementInMB -= dataToMove;
      _finishedInterBrokerDataMovementInMB += dataToMove;
    }
  }

  /**
   * Add new tasks to ExecutionTaskTracker to trace their execution.
   * Tasks are added homogeneously -- all tasks have the same task type.
   *
   * @param tasks    New tasks to add.
   * @param taskType Task type of new tasks.
   */
  public void addTasksToTrace(Collection<ExecutionTask> tasks, TaskType taskType) {
    _tasksByType.get(taskType).get(State.PENDING).addAll(tasks);
    if (taskType == TaskType.INTER_BROKER_REPLICA_ACTION) {
      _remainingInterBrokerDataToMoveInMB += tasks.stream().mapToLong(t -> t.proposal().dataToMoveInMB()).sum();
    }
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   */
  public void setExecutionMode(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Get the statistic of task execution state.
   *
   * @return The statistic of task execution state.
   */
  private Map<TaskType, Map<State, Integer>> taskStat() {
    Map<TaskType, Map<State, Integer>> taskStatMap = new HashMap<>(TaskType.cachedValues().size());
    for (TaskType type : TaskType.cachedValues()) {
      taskStatMap.put(type, new HashMap<>());
      _tasksByType.get(type).forEach((k, v) -> taskStatMap.get(type).put(k, v.size()));
    }
    return taskStatMap;
  }

  /**
   * Get a filtered list of tasks of different {@link TaskType} and in different {@link State}.
   *
   * @param taskTypesToGetFullList  Task types to return complete list of tasks.
   * @return                        A filtered list of tasks.
   */
  private Map<TaskType, Map<State, Set<ExecutionTask>>> filteredTasksByState(Set<TaskType> taskTypesToGetFullList) {
    Map<TaskType, Map<State, Set<ExecutionTask>>> tasksByState = new HashMap<>(taskTypesToGetFullList.size());
    for (TaskType type : taskTypesToGetFullList) {
      tasksByState.put(type, new HashMap<>());
      _tasksByType.get(type).forEach((k, v) -> {
          tasksByState.get(type).put(k, new HashSet<>(v));
      });
    }
    return tasksByState;
  }

  /**
   * Clear the replica action and leader action tasks.
   */
  public void clear() {
    _tasksByType.values().forEach(m -> m.values().forEach(Set::clear));
    _remainingInterBrokerDataToMoveInMB = 0L;
    _inExecutionInterBrokerDataMovementInMB = 0L;
    _finishedInterBrokerDataMovementInMB = 0L;
  }

  // Internal query APIs.
  public int numRemainingInterBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(State.PENDING).size();
  }

  public long remainingInterBrokerDataToMoveInMB() {
    return _remainingInterBrokerDataToMoveInMB;
  }

  public int numFinishedInterBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(State.COMPLETED).size() +
           _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(State.DEAD).size() +
           _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(State.ABORTED).size();
  }

  public long finishedInterBrokerDataMovementInMB() {
    return _finishedInterBrokerDataMovementInMB;
  }

  public Set<ExecutionTask> inExecutionTasks(Collection<TaskType> types) {
    Set<ExecutionTask> inExecutionTasks = new HashSet<>();
    for (TaskType type : types) {
      inExecutionTasks.addAll(_tasksByType.get(type).get(State.IN_PROGRESS));
      inExecutionTasks.addAll(_tasksByType.get(type).get(State.ABORTING));
    }
    return inExecutionTasks;
  }

  public long inExecutionInterBrokerDataMovementInMB() {
    return _inExecutionInterBrokerDataMovementInMB;
  }

  public int numRemainingLeadershipMovements() {
    return _tasksByType.get(TaskType.LEADER_ACTION).get(State.PENDING).size();
  }

  public int numFinishedLeadershipMovements() {
    return _tasksByType.get(TaskType.LEADER_ACTION).get(State.COMPLETED).size() +
           _tasksByType.get(TaskType.LEADER_ACTION).get(State.DEAD).size() +
           _tasksByType.get(TaskType.LEADER_ACTION).get(State.ABORTED).size();
  }

  public ExecutionTasksSummary getExecutionTasksSummary(Set<TaskType> taskTypesToGetFullList) {
    return new ExecutionTasksSummary(_finishedInterBrokerDataMovementInMB,
                                     _inExecutionInterBrokerDataMovementInMB,
                                     _remainingInterBrokerDataToMoveInMB,
                                     taskStat(),
                                     filteredTasksByState(taskTypesToGetFullList));
  }

  public static class ExecutionTasksSummary {
    private long _finishedInterBrokerDataMovementInMB;
    private long _inExecutionInterBrokerDataMovementInMB;
    private final long _remainingInterBrokerDataToMoveInMB;
    private Map<TaskType, Map<State, Integer>> _taskStat;
    private Map<TaskType, Map<State, Set<ExecutionTask>>> _filteredTasksByState;

    private ExecutionTasksSummary(long finishedInterBrokerDataMovementInMB,
                                  long inExecutionInterBrokerDataMovementInMB,
                                  long remainingInterBrokerDataToMoveInMB,
                                  Map<TaskType, Map<State, Integer>> taskStat,
                                  Map<TaskType, Map<State, Set<ExecutionTask>>> filteredTasksByState) {
      _finishedInterBrokerDataMovementInMB = finishedInterBrokerDataMovementInMB;
      _inExecutionInterBrokerDataMovementInMB = inExecutionInterBrokerDataMovementInMB;
      _remainingInterBrokerDataToMoveInMB = remainingInterBrokerDataToMoveInMB;
      _taskStat = taskStat;
      _filteredTasksByState = filteredTasksByState;
    }

    public long finishedInterBrokerDataMovementInMB() {
      return _finishedInterBrokerDataMovementInMB;
    }

    public long inExecutionInterBrokerDataMovementInMB() {
      return _inExecutionInterBrokerDataMovementInMB;
    }

    public long remainingInterBrokerDataToMoveInMB() {
      return _remainingInterBrokerDataToMoveInMB;
    }

    public Map<TaskType, Map<State, Integer>> taskStat() {
      return _taskStat;
    }

    public Map<TaskType, Map<State, Set<ExecutionTask>>> filteredTasksByState() {
      return _filteredTasksByState;
    }
  }
}