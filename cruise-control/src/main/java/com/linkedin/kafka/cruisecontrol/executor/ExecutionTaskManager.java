/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;
/**
 * The class that helps track the execution status for the balancing.
 * It does the following things:
 * 1. Keep track of the in progress partition movements between each pair of source-destination disk or broker.
 * 2. When one partition movement finishes, it checks the involved brokers to see if we can run more partition movements.
 * We only keep track of the number of concurrent partition movements but not the sizes of the partitions.
 * Because the concurrent level determines how much impact the balancing process would have on the involved
 * brokers. And the size of partitions only affect how long the impact would last.
 *
 * The execution task manager is thread-safe.
 */
public class ExecutionTaskManager {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionTaskManager.class);
  private final Map<Integer, Integer> _inProgressInterBrokerReplicaMovementsByBrokerId;
  private final Map<Integer, Integer> _inProgressIntraBrokerReplicaMovementsByBrokerId;
  private final Set<TopicPartition> _inProgressPartitionsForInterBrokerMovement;
  private final ExecutionTaskTracker _executionTaskTracker;
  private final ExecutionTaskPlanner _executionTaskPlanner;
  private final int _defaultInterBrokerPartitionMovementConcurrency;
  private Integer _requestedInterBrokerPartitionMovementConcurrency;
  private final int _defaultIntraBrokerPartitionMovementConcurrency;
  private Integer _requestedIntraBrokerPartitionMovementConcurrency;
  private final int _defaultLeadershipMovementConcurrency;
  private final int _maxNumClusterMovementConcurrency;
  private Integer _requestedLeadershipMovementConcurrency;
  private final Set<Integer> _brokersToSkipConcurrencyCheck;
  private boolean _isKafkaAssignerMode;

  /**
   * The constructor of The Execution task manager.
   *
   * @param adminClient The adminClient use to query logdir information of replicas.
   * @param dropwizardMetricRegistry The metric registry.
   * @param time The time object to get the time.
   * @param config config object that holds all Kafka Cruise control related configs
   */
  public ExecutionTaskManager(AdminClient adminClient,
                              MetricRegistry dropwizardMetricRegistry,
                              Time time,
                              KafkaCruiseControlConfig config) {
    _inProgressInterBrokerReplicaMovementsByBrokerId = new HashMap<>();
    _inProgressIntraBrokerReplicaMovementsByBrokerId = new HashMap<>();
    _inProgressPartitionsForInterBrokerMovement = new HashSet<>();
    _executionTaskTracker = new ExecutionTaskTracker(dropwizardMetricRegistry, time);
    _executionTaskPlanner = new ExecutionTaskPlanner(adminClient, config);
    _defaultInterBrokerPartitionMovementConcurrency = config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG);
    _defaultIntraBrokerPartitionMovementConcurrency = config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG);
    _defaultLeadershipMovementConcurrency = config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG);
    _maxNumClusterMovementConcurrency = config.getInt(KafkaCruiseControlConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    _brokersToSkipConcurrencyCheck = new HashSet<>();
    _isKafkaAssignerMode = false;
    _requestedInterBrokerPartitionMovementConcurrency = null;
    _requestedIntraBrokerPartitionMovementConcurrency = null;
    _requestedLeadershipMovementConcurrency = null;
  }

  /**
   * Dynamically set the inter-broker partition movement concurrency per broker.
   * Ensure that the requested concurrency is smaller than the maximum number of allowed movements in cluster.
   *
   * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements per broker
   *                                                         (if null, use {@link #_defaultInterBrokerPartitionMovementConcurrency}).
   */
  public synchronized void setRequestedInterBrokerPartitionMovementConcurrency(Integer requestedInterBrokerPartitionMovementConcurrency) {
    if (requestedInterBrokerPartitionMovementConcurrency != null
        && requestedInterBrokerPartitionMovementConcurrency >= _maxNumClusterMovementConcurrency) {
      throw new IllegalArgumentException("Attempt to set inter-broker partition movement concurrency ["
                                         + requestedInterBrokerPartitionMovementConcurrency + "] to greater than or equal to the maximum"
                                         + " number of allowed movements in cluster [" + _maxNumClusterMovementConcurrency + "].");
    }
    _requestedInterBrokerPartitionMovementConcurrency = requestedInterBrokerPartitionMovementConcurrency;
  }

  /**
   * Dynamically set the intra-broker partition movement concurrency.
   * Ensure that the requested concurrency is smaller than the maximum number of allowed movements in cluster.
   *
   * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements
   *                                                         (if null, use {@link #_defaultIntraBrokerPartitionMovementConcurrency}).
   */
  public synchronized void setRequestedIntraBrokerPartitionMovementConcurrency(Integer requestedIntraBrokerPartitionMovementConcurrency) {
    if (requestedIntraBrokerPartitionMovementConcurrency != null
        && requestedIntraBrokerPartitionMovementConcurrency >= _maxNumClusterMovementConcurrency) {
      throw new IllegalArgumentException("Attempt to set intra-broker partition movement concurrency ["
                                         + requestedIntraBrokerPartitionMovementConcurrency + "] to greater than or equal to the maximum"
                                         + " number of allowed movements in cluster [" + _maxNumClusterMovementConcurrency + "].");
    }
    _requestedIntraBrokerPartitionMovementConcurrency = requestedIntraBrokerPartitionMovementConcurrency;
  }

  /**
   * Dynamically set the leadership movement concurrency.
   * Ensure that the requested concurrency is not greater than the maximum number of allowed movements in cluster.
   *
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements
   *                                               (if null, {@link #_defaultLeadershipMovementConcurrency}).
   */
  public synchronized void setRequestedLeadershipMovementConcurrency(Integer requestedLeadershipMovementConcurrency) {
    if (requestedLeadershipMovementConcurrency != null
        && requestedLeadershipMovementConcurrency > _maxNumClusterMovementConcurrency) {
      throw new IllegalArgumentException("Attempt to set leadership movement concurrency ["
                                         + requestedLeadershipMovementConcurrency + "] to greater than the maximum number "
                                         + "of allowed movements in cluster [" + _maxNumClusterMovementConcurrency + "].");
    }
    _requestedLeadershipMovementConcurrency = requestedLeadershipMovementConcurrency;
  }

  public synchronized int interBrokerPartitionMovementConcurrency() {
    return _requestedInterBrokerPartitionMovementConcurrency == null ? _defaultInterBrokerPartitionMovementConcurrency
                                                                     : _requestedInterBrokerPartitionMovementConcurrency;
  }

  public synchronized int intraBrokerPartitionMovementConcurrency() {
    return _requestedIntraBrokerPartitionMovementConcurrency == null ? _defaultIntraBrokerPartitionMovementConcurrency
                                                                     : _requestedIntraBrokerPartitionMovementConcurrency;
  }

  public synchronized int leadershipMovementConcurrency() {
    return _requestedLeadershipMovementConcurrency == null ? _defaultLeadershipMovementConcurrency
                                                           : _requestedLeadershipMovementConcurrency;
  }

  /**
   * Returns a list of execution tasks that move the replicas cross brokers.
   */
  public synchronized List<ExecutionTask> getInterBrokerReplicaMovementTasks() {
    Map<Integer, Integer> brokersReadyForReplicaMovement = brokersReadyForReplicaMovement(_inProgressInterBrokerReplicaMovementsByBrokerId,
                                                                                          interBrokerPartitionMovementConcurrency());
    return _executionTaskPlanner.getInterBrokerReplicaMovementTasks(brokersReadyForReplicaMovement, _inProgressPartitionsForInterBrokerMovement);
  }

  /**
   * Returns a list of execution tasks that move the replicas cross disks of the same broker.
   */
  public synchronized List<ExecutionTask> getIntraBrokerReplicaMovementTasks() {
    Map<Integer, Integer> brokersReadyForReplicaMovement = brokersReadyForReplicaMovement(_inProgressIntraBrokerReplicaMovementsByBrokerId,
                                                                                          intraBrokerPartitionMovementConcurrency());
    return _executionTaskPlanner.getIntraBrokerReplicaMovementTasks(brokersReadyForReplicaMovement);
  }

  private int unthrottledConcurrency(Set<Integer> brokersWithReplicaMoves, int throttledConcurrency) {
    int numUnthrottledBrokers = (int) brokersWithReplicaMoves.stream().filter(_brokersToSkipConcurrencyCheck::contains).count();
    if (numUnthrottledBrokers == 0) {
      // All brokers are throttled.
      return Integer.MAX_VALUE;
    }

    int numThrottledBrokers = brokersWithReplicaMoves.size() - numUnthrottledBrokers;
    return Math.max(1, (_maxNumClusterMovementConcurrency - (numThrottledBrokers * throttledConcurrency)) / numUnthrottledBrokers);
  }

  /**
   * Based on replica movement concurrency requirement and number of ongoing replica movements, calculate how many
   * new replica movements can be triggered on each broker.
   *
   * @param inProgressReplicaMovementsByBrokerId Number of ongoing replica movements in each broker.
   * @param throttledConcurrency The throttled concurrency of per-broker replica movement.
   * @return A map of how many new replica movements can be triggered for each broker.
   */
  private Map<Integer, Integer> brokersReadyForReplicaMovement(Map<Integer, Integer> inProgressReplicaMovementsByBrokerId,
                                                               int throttledConcurrency) {
    int totalThrottledPartitionMovementConcurrency = inProgressReplicaMovementsByBrokerId.size() * throttledConcurrency;
    if (totalThrottledPartitionMovementConcurrency > _maxNumClusterMovementConcurrency) {
      LOG.error("Total throttled partition movement concurrency ({}) is greater than the maximum number of allowed "
                + " movements in cluster ({}). Please decrease the per-broker partition movement concurrency ({}) to avoid "
                + "a potential ZooKeeper zNode file size limit violation during replica moves.",
                totalThrottledPartitionMovementConcurrency, _maxNumClusterMovementConcurrency, throttledConcurrency);
    }
    Map<Integer, Integer> readyBrokers = new HashMap<>(inProgressReplicaMovementsByBrokerId.size());
    int unthrottledConcurrency = unthrottledConcurrency(inProgressReplicaMovementsByBrokerId.keySet(), throttledConcurrency);
    inProgressReplicaMovementsByBrokerId.forEach((bid, inProgressReplicaMovements) -> {
      int brokerConcurrency = _brokersToSkipConcurrencyCheck.contains(bid) ? unthrottledConcurrency : throttledConcurrency;
      readyBrokers.put(bid, Math.max(0, brokerConcurrency - inProgressReplicaMovements));
    });

    return readyBrokers;
  }

  /**
   * Returns a list of execution tasks that move the leadership.
   */
  public synchronized List<ExecutionTask> getLeadershipMovementTasks() {
    return _executionTaskPlanner.getLeadershipMovementTasks(leadershipMovementConcurrency());
  }

  /**
   * Add a collection of execution proposals for execution. The method allows users to skip the concurrency check
   * on some given brokers. Notice that this method will replace the existing brokers that were in the concurrency
   * check privilege state with the new broker set.
   *
   * @param proposals the execution proposals to execute.
   * @param brokersToSkipConcurrencyCheck Brokers that do not need to be throttled when moving the partitions. Note that
   *                                      there would still be some throttling based on {@link #_maxNumClusterMovementConcurrency}
   *                                      to ensure that default ZooKeeper zNode file size limit is not exceeded.
   * @param cluster Cluster state.
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   */
  public synchronized void addExecutionProposals(Collection<ExecutionProposal> proposals,
                                                 Collection<Integer> brokersToSkipConcurrencyCheck,
                                                 Cluster cluster,
                                                 ReplicaMovementStrategy replicaMovementStrategy) {
    _executionTaskPlanner.addExecutionProposals(proposals, cluster, replicaMovementStrategy);
    for (ExecutionProposal p : proposals) {
      p.replicasToMoveBetweenDisksByBroker().keySet()
                                            .forEach(broker -> _inProgressIntraBrokerReplicaMovementsByBrokerId.putIfAbsent(broker, 0));
      _inProgressInterBrokerReplicaMovementsByBrokerId.putIfAbsent(p.oldLeader().brokerId(), 0);
      p.replicasToAdd().forEach(r -> _inProgressInterBrokerReplicaMovementsByBrokerId.putIfAbsent(r.brokerId(), 0));
    }
    // Set the execution mode for tasks.
    _executionTaskTracker.setExecutionMode(_isKafkaAssignerMode);

    // Populate the generated tasks to tracker to trace their execution.
    _executionTaskTracker.addTasksToTrace(_executionTaskPlanner.remainingInterBrokerReplicaMovements(), TaskType.INTER_BROKER_REPLICA_ACTION);
    _executionTaskTracker.addTasksToTrace(_executionTaskPlanner.remainingIntraBrokerReplicaMovements(), TaskType.INTRA_BROKER_REPLICA_ACTION);
    _executionTaskTracker.addTasksToTrace(_executionTaskPlanner.remainingLeadershipMovements(), TaskType.LEADER_ACTION);
    _brokersToSkipConcurrencyCheck.clear();
    if (brokersToSkipConcurrencyCheck != null) {
      _brokersToSkipConcurrencyCheck.addAll(brokersToSkipConcurrencyCheck);
    }
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   */
  public synchronized void setExecutionModeForTaskTracker(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Mark the given tasks as in progress. Tasks are executed homogeneously -- all tasks have the same balancing action.
   */
  public synchronized void markTasksInProgress(List<ExecutionTask> tasks) {
    if (!tasks.isEmpty()) {
      for (ExecutionTask task : tasks) {
        _executionTaskTracker.markTaskState(task, State.IN_PROGRESS);
        switch (task.type()) {
          case INTER_BROKER_REPLICA_ACTION:
            _inProgressPartitionsForInterBrokerMovement.add(task.proposal().topicPartition());
            int oldLeader = task.proposal().oldLeader().brokerId();
            _inProgressInterBrokerReplicaMovementsByBrokerId.put(oldLeader,
                                                                 _inProgressInterBrokerReplicaMovementsByBrokerId.get(oldLeader) + 1);
            task.proposal()
                .replicasToAdd()
                .forEach(r -> _inProgressInterBrokerReplicaMovementsByBrokerId.put(r.brokerId(),
                              _inProgressInterBrokerReplicaMovementsByBrokerId.get(r.brokerId()) + 1));
            break;
          case INTRA_BROKER_REPLICA_ACTION:
            _inProgressIntraBrokerReplicaMovementsByBrokerId.put(task.brokerId(),
                                                                 _inProgressIntraBrokerReplicaMovementsByBrokerId.get(task.brokerId()) + 1);
            break;
          default:
            break;
        }
      }
    }
  }

  /**
   * Mark the successful completion of a given task. In-progress execution will yield successful completion.
   * Aborting execution will yield Aborted completion.
   */
  public synchronized void markTaskDone(ExecutionTask task) {
    if (task.state() == State.IN_PROGRESS) {
      _executionTaskTracker.markTaskState(task, State.COMPLETED);
      completeTask(task);
    } else if (task.state() == State.ABORTING) {
      _executionTaskTracker.markTaskState(task, State.ABORTED);
      completeTask(task);
    }
  }

  /**
   * Mark an in-progress task as aborting (1) if an error is encountered and (2) the rollback is possible.
   */
  public synchronized void markTaskAborting(ExecutionTask task) {
    if (task.state() == State.IN_PROGRESS) {
      _executionTaskTracker.markTaskState(task, State.ABORTING);
    }
  }

  /**
   * Mark an in-progress task as aborting (1) if an error is encountered and (2) the rollback is not possible.
   */
  public synchronized void markTaskDead(ExecutionTask task) {
    if (task.state() != State.DEAD) {
      _executionTaskTracker.markTaskState(task, State.DEAD);
      completeTask(task);
    }
  }

  /**
   * Mark a given tasks as completed.
   */
  private void completeTask(ExecutionTask task) {
    switch (task.type()) {
      case INTER_BROKER_REPLICA_ACTION:
        _inProgressPartitionsForInterBrokerMovement.remove(task.proposal().topicPartition());
        int oldLeader = task.proposal().oldLeader().brokerId();
        _inProgressInterBrokerReplicaMovementsByBrokerId.put(oldLeader,
                                                             _inProgressInterBrokerReplicaMovementsByBrokerId.get(oldLeader) - 1);
        task.proposal()
            .replicasToAdd()
            .forEach(r -> _inProgressInterBrokerReplicaMovementsByBrokerId.put(r.brokerId(),
                          _inProgressInterBrokerReplicaMovementsByBrokerId.get(r.brokerId()) - 1));
        break;
      case INTRA_BROKER_REPLICA_ACTION:
        _inProgressIntraBrokerReplicaMovementsByBrokerId.put(task.brokerId(),
                                                             _inProgressIntraBrokerReplicaMovementsByBrokerId.get(task.brokerId()) - 1);
        break;
      default:
        // No-op for other type of task, i.e LEADER_ACTION.
        break;
    }
  }

  public synchronized int numRemainingInterBrokerPartitionMovements() {
    return _executionTaskTracker.numRemainingInterBrokerPartitionMovements();
  }

  public synchronized long remainingInterBrokerDataToMoveInMB() {
    return _executionTaskTracker.remainingInterBrokerDataToMoveInMB();
  }

  public synchronized int numFinishedInterBrokerPartitionMovements() {
    return _executionTaskTracker.numFinishedInterBrokerPartitionMovements();
  }

  public synchronized long finishedInterBrokerDataMovementInMB() {
    return _executionTaskTracker.finishedInterBrokerDataMovementInMB();
  }

  public synchronized Set<ExecutionTask> inExecutionTasks() {
    return inExecutionTasks(TaskType.cachedValues());
  }

  public synchronized Set<ExecutionTask> inExecutionTasks(Collection<TaskType> types) {
    return _executionTaskTracker.inExecutionTasks(types);
  }

  public synchronized long inExecutionInterBrokerDataToMoveInMB() {
    return _executionTaskTracker.inExecutionInterBrokerDataMovementInMB();
  }

  public synchronized int numRemainingLeadershipMovements() {
    return _executionTaskTracker.numRemainingLeadershipMovements();
  }

  public synchronized int numFinishedLeadershipMovements() {
    return _executionTaskTracker.numFinishedLeadershipMovements();
  }

  public synchronized int numRemainingIntraBrokerPartitionMovements() {
    return _executionTaskTracker.numRemainingIntraBrokerPartitionMovements();
  }

  public synchronized long remainingIntraBrokerDataToMoveInMB() {
    return _executionTaskTracker.remainingIntraBrokerDataToMoveInMB();
  }

  public synchronized int numFinishedIntraBrokerPartitionMovements() {
    return _executionTaskTracker.numFinishedIntraBrokerPartitionMovements();
  }

  public synchronized long finishedIntraBrokerDataToMoveInMB() {
    return _executionTaskTracker.finishedIntraBrokerDataToMoveInMB();
  }

  public long inExecutionIntraBrokerDataMovementInMB() {
    return _executionTaskTracker.inExecutionIntraBrokerDataMovementInMB();
  }

  public synchronized void clear() {
    _brokersToSkipConcurrencyCheck.clear();
    _inProgressInterBrokerReplicaMovementsByBrokerId.clear();
    _inProgressIntraBrokerReplicaMovementsByBrokerId.clear();
    _inProgressPartitionsForInterBrokerMovement.clear();
    _executionTaskPlanner.clear();
    _executionTaskTracker.clear();
  }

  public synchronized void setStopRequested() {
    _executionTaskTracker.setStopRequested();
  }

  public synchronized ExecutionTasksSummary getExecutionTasksSummary(Set<TaskType> taskTypesToGetFullList) {
    return _executionTaskTracker.getExecutionTasksSummary(taskTypesToGetFullList);
  }
}