/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.*;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

/**
 * The class holds the execution of balance proposals for rebalance.
 * <p>
 * Each proposal is processed and may generate one leadership movement, one inter-broker partition movement and several
 * intra-broker partition movement tasks. Each task is assigned an execution id and managed in two ways.
 * <ul>
 * <li>All leadership movement tasks are put into the same list and will be executed together.
 * <li>Partition movement tasks are tracked by broker and execution Id.
 * </ul>
 * <p>
 * This class tracks the partition movements for each broker using a sorted Set.
 * The task's position in this set represents its execution order. For inter-broker partition movement task, the position
 * is determined by the passed in {@link ReplicaMovementStrategy} or {@link ExecutionTaskPlanner#_defaultReplicaMovementTaskStrategy}.
 * For intra-broker partition movement task, the position is determined by assigned execution id in ascending order.
 * The task is tracked both under source broker and destination broker's plan.
 * Once a task is fulfilled, the task will be removed from both source broker and destination broker's execution plan.
 * <p>
 * This class is not thread safe.
 */
public class ExecutionTaskPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionTaskPlanner.class);
  private Map<Integer, SortedSet<ExecutionTask>> _interPartMoveTaskByBrokerId;
  private Map<Integer, SortedSet<ExecutionTask>> _intraPartMoveTaskByBrokerId;
  private final Set<ExecutionTask> _remainingInterBrokerReplicaMovements;
  private final Set<ExecutionTask> _remainingIntraBrokerReplicaMovements;
  private final Map<Long, ExecutionTask> _remainingLeadershipMovements;
  private long _executionId;
  private ReplicaMovementStrategy _defaultReplicaMovementTaskStrategy;
  private final AdminClient _adminClient;
  private final KafkaCruiseControlConfig _config;
  private final long _taskExecutionAlertingThresholdMs;
  private final double _interBrokerReplicaMovementRateAlertingThreshold;
  private final double _intraBrokerReplicaMovementRateAlertingThreshold;

  /**
   *
   * @param adminClient The adminClient to send describeReplicaLogDirs request.
   * @param config The config object that holds all the Cruise Control related configs.
   */
  public ExecutionTaskPlanner(AdminClient adminClient, KafkaCruiseControlConfig config) {
    _executionId = 0L;
    _interPartMoveTaskByBrokerId = new HashMap<>();
    _intraPartMoveTaskByBrokerId = new HashMap<>();
    _remainingInterBrokerReplicaMovements = new HashSet<>();
    _remainingIntraBrokerReplicaMovements = new HashSet<>();
    _remainingLeadershipMovements = new HashMap<>();
    _config = config;
    _taskExecutionAlertingThresholdMs = config.getLong(TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG);
    _interBrokerReplicaMovementRateAlertingThreshold = config.getDouble(INTER_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG);
    _intraBrokerReplicaMovementRateAlertingThreshold = config.getDouble(INTRA_BROKER_REPLICA_MOVEMENT_RATE_ALERTING_THRESHOLD_CONFIG);
    _adminClient = adminClient;
    List<String> defaultReplicaMovementStrategies = config.getList(DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG);
    if (defaultReplicaMovementStrategies == null || defaultReplicaMovementStrategies.isEmpty()) {
      _defaultReplicaMovementTaskStrategy = new BaseReplicaMovementStrategy();
    } else {
      for (String replicaMovementStrategy : defaultReplicaMovementStrategies) {
        try {
          if (_defaultReplicaMovementTaskStrategy == null) {
            _defaultReplicaMovementTaskStrategy = (ReplicaMovementStrategy) Class.forName(replicaMovementStrategy).newInstance();
          } else {
            _defaultReplicaMovementTaskStrategy = _defaultReplicaMovementTaskStrategy.chain(
                (ReplicaMovementStrategy) Class.forName(replicaMovementStrategy).newInstance());
          }
        } catch (Exception e) {
          throw new RuntimeException("Error occurred while setting up the replica movement strategy: " + replicaMovementStrategy + ".", e);
        }
      }
      // Chain the custom strategies with BaseReplicaMovementStrategy in the end to handle the scenario that provided custom strategy is unable
      // to determine the order of two tasks. BaseReplicaMovementStrategy makes the task with smaller execution id to get executed first.
      _defaultReplicaMovementTaskStrategy = _defaultReplicaMovementTaskStrategy.chain(new BaseReplicaMovementStrategy());
    }
  }

  /**
   * Add each given proposal to execute, unless the given cluster state indicates that the proposal would be a no-op.
   * A proposal is a no-op if the expected state after the execution of the given proposal is the current cluster state.
   * The proposal to be added will have at least one of the three following actions:
   * 1. Inter-broker replica action (i.e. movement, addition, deletion or order change).
   * 2. Intra-broker replica action (i.e. movement).
   * 3. Leader action (i.e. leadership movement)
   *
   * @param proposals Execution proposals.
   * @param cluster Kafka cluster state.
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   */
  public void addExecutionProposals(Collection<ExecutionProposal> proposals,
                                    Cluster cluster,
                                    ReplicaMovementStrategy replicaMovementStrategy) {
    LOG.trace("Cluster state before adding proposals: {}.", cluster);
    maybeAddInterBrokerReplicaMovementTasks(proposals, cluster, replicaMovementStrategy);
    maybeAddIntraBrokerReplicaMovementTasks(proposals);
    maybeAddLeaderChangeTasks(proposals, cluster);
    sanityCheckExecutionTasks();
    maybeDropReplicaSwapTasks();
  }

  /**
   * Sanity check that if there is any intra-broker partition movement task generated, no inter-broker partition movement task
   * is generated.
   */
  private void sanityCheckExecutionTasks() {
    if (_remainingIntraBrokerReplicaMovements.size() > 0) {
      for (ExecutionTask task : _remainingInterBrokerReplicaMovements) {
        if (task.proposal().replicasToAdd().size() > 0) {
          throw new IllegalStateException("Intra-broker partition movement should not mingle with inter-broker partition movement.");
        }
      }
    }
  }

  /**
   * If there is any intra-broker partition movement task generated, any inter-broker replica swap operation is cancelled.
   * The reason to do this is because disk rebalance goals may generate some inter-broker replica swap operations.
   * These swap operations do not improve the disk load balance but bring a potential risk of hang in execution(if the cluster
   * has some dead disks and the generated swap operation involves offline replica).
   */
  private void maybeDropReplicaSwapTasks() {
    if (_remainingIntraBrokerReplicaMovements.size() > 0) {
      _interPartMoveTaskByBrokerId.clear();
      _remainingInterBrokerReplicaMovements.clear();
    }
  }

  /**
   * For each proposal, create a replica action task if there is a need for moving replica(s) between brokers to reach
   * expected final proposal state.
   *
   * @param proposals Execution proposals.
   * @param cluster Kafka cluster state.
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   */
  private void maybeAddInterBrokerReplicaMovementTasks(Collection<ExecutionProposal> proposals,
                                                       Cluster cluster,
                                                       ReplicaMovementStrategy replicaMovementStrategy) {
    for (ExecutionProposal proposal : proposals) {
      TopicPartition tp = proposal.topicPartition();
      PartitionInfo partitionInfo = cluster.partition(tp);
      if (partitionInfo == null) {
        LOG.trace("Ignored the attempt to move non-existing partition for topic partition: {}", tp);
        continue;
      }
      if (!proposal.isInterBrokerMovementCompleted(partitionInfo)) {
        long replicaActionExecutionId = _executionId++;
        long executionAlertingThresholdMs = Math.max(Math.round(proposal.dataToMoveInMB() / _interBrokerReplicaMovementRateAlertingThreshold),
                                                     _taskExecutionAlertingThresholdMs);
        ExecutionTask executionTask = new ExecutionTask(replicaActionExecutionId, proposal, INTER_BROKER_REPLICA_ACTION,
                                                        executionAlertingThresholdMs);
        _remainingInterBrokerReplicaMovements.add(executionTask);
        LOG.trace("Added action {} as replica proposal {}", replicaActionExecutionId, proposal);
      }
    }
    if (replicaMovementStrategy == null) {
      _interPartMoveTaskByBrokerId = _defaultReplicaMovementTaskStrategy.applyStrategy(_remainingInterBrokerReplicaMovements,
                                                                                       cluster);
    } else {
      // Chain the generated composite strategy with BaseReplicaMovementStrategy in the end to ensure the returned
      // strategy can always determine the order of two tasks.
      _interPartMoveTaskByBrokerId = replicaMovementStrategy.chain(new BaseReplicaMovementStrategy())
              .applyStrategy(_remainingInterBrokerReplicaMovements, cluster);
    }
  }

  /**
   * For each proposal, create a replica action task if there is a need for moving replica(s) between disks of the broker
   * to reach expected final proposal state.
   *
   * @param proposals Execution proposals.
   */
  private void maybeAddIntraBrokerReplicaMovementTasks(Collection<ExecutionProposal> proposals) {
    Set<TopicPartitionReplica> replicasToCheckLogdir = new HashSet<>();
    for (ExecutionProposal proposal : proposals) {
      proposal.replicasToMoveBetweenDisksByBroker().keySet().forEach(broker ->
        replicasToCheckLogdir.add(new TopicPartitionReplica(proposal.topic(), proposal.partitionId(), broker)));
    }

    if (!replicasToCheckLogdir.isEmpty()) {
      Map<TopicPartitionReplica, String> currentLogdirByReplica = new HashMap<>(replicasToCheckLogdir.size());
      Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> logDirsByReplicas =
          _adminClient.describeReplicaLogDirs(replicasToCheckLogdir).values();
      for (Map.Entry<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> entry : logDirsByReplicas.entrySet()) {
        try {
          ReplicaLogDirInfo info = entry.getValue().get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
          currentLogdirByReplica.put(entry.getKey(), info.getCurrentReplicaLogDir());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          LOG.warn("Encounter exception {} when fetching logdir information for replica {}.", e.getMessage(), entry.getKey());
        }
      }

      for (ExecutionProposal proposal : proposals) {
        proposal.replicasToMoveBetweenDisksByBroker().values().forEach(r -> {
          String currentLogdir = currentLogdirByReplica.get(new TopicPartitionReplica(proposal.topic(), proposal.partitionId(), r.brokerId()));
          if (currentLogdir != null && !currentLogdir.equals(r.logdir())) {
            long replicaActionExecutionId = _executionId++;
            ExecutionTask task = new ExecutionTask(replicaActionExecutionId, proposal, r.brokerId(), INTRA_BROKER_REPLICA_ACTION,
                                                   Math.max(Math.round(proposal.dataToMoveInMB() / _intraBrokerReplicaMovementRateAlertingThreshold),
                                                            _taskExecutionAlertingThresholdMs));
            _intraPartMoveTaskByBrokerId.putIfAbsent(r.brokerId(), new TreeSet<>());
            _intraPartMoveTaskByBrokerId.get(r.brokerId()).add(task);
            _remainingIntraBrokerReplicaMovements.add(task);
          }
        });
      }
    }
  }

  /**
   * For each proposal, create a leader action task if there is a need for moving the leadership to reach expected final proposal state.
   *
   * @param proposals Execution proposals.
   * @param cluster Kafka cluster state.
   */
  private void maybeAddLeaderChangeTasks(Collection<ExecutionProposal> proposals, Cluster cluster) {
    for (ExecutionProposal proposal : proposals) {
      if (proposal.hasLeaderAction()) {
        Node currentLeader = cluster.leaderFor(proposal.topicPartition());
        if (currentLeader != null && currentLeader.id() != proposal.newLeader().brokerId()) {
          // Get the execution Id for the leader action proposal execution;
          long leaderActionExecutionId = _executionId++;
          ExecutionTask leaderActionTask = new ExecutionTask(leaderActionExecutionId, proposal, LEADER_ACTION, _taskExecutionAlertingThresholdMs);
          _remainingLeadershipMovements.put(leaderActionExecutionId, leaderActionTask);
          LOG.trace("Added action {} as leader proposal {}", leaderActionExecutionId, proposal);
        }
      }
    }
  }

  /**
   * @return The remaining inter-broker replica movement tasks.
   */
  public Set<ExecutionTask> remainingInterBrokerReplicaMovements() {
    return _remainingInterBrokerReplicaMovements;
  }

  /**
   * @return The remaining intra-broker replica movement tasks.
   */
  public Set<ExecutionTask> remainingIntraBrokerReplicaMovements() {
    return _remainingIntraBrokerReplicaMovements;
  }

  /**
   * @return The remaining leadership movements.
   */
  public Collection<ExecutionTask> remainingLeadershipMovements() {
    return _remainingLeadershipMovements.values();
  }

  /**
   * Get the leadership movement tasks, and remove them from _remainingLeadershipMovements.
   *
   * @param numTasks Number of tasks to remove from the _remainingLeadershipMovements. If _remainingLeadershipMovements
   *                 has less than numTasks, all tasks are removed.
   * @return The leadership movement tasks.
   */
  public List<ExecutionTask> getLeadershipMovementTasks(int numTasks) {
    List<ExecutionTask> leadershipMovementsList = new ArrayList<>();
    Iterator<ExecutionTask> leadershipMovementIter = _remainingLeadershipMovements.values().iterator();
    for (int i = 0; i < numTasks && leadershipMovementIter.hasNext(); i++) {
      leadershipMovementsList.add(leadershipMovementIter.next());
      leadershipMovementIter.remove();
    }
    return leadershipMovementsList;
  }

  /**
   * Get a list of executable inter-broker replica movements that comply with the concurrency constraint.
   *
   * @param readyBrokers The brokers that is ready to execute more movements.
   * @param inProgressPartitions Topic partitions of replicas that are already in progress. This is needed because the
   *                             controller does not allow updating the ongoing replica reassignment for a partition
   *                             whose replica is being reassigned.
   * @return A list of movements that is executable for the ready brokers.
   */
  public List<ExecutionTask> getInterBrokerReplicaMovementTasks(Map<Integer, Integer> readyBrokers,
                                                                Set<TopicPartition> inProgressPartitions) {
    LOG.trace("Getting inter-broker replica movement tasks for brokers with concurrency {}", readyBrokers);
    List<ExecutionTask> executableReplicaMovements = new ArrayList<>();

    /**
     * The algorithm avoids unfair situation where the available movement slots of a broker is completely taken
     * by another broker. It checks the proposals in a round-robin manner that makes sure each ready broker gets
     * chances to make progress.
     */
    boolean newTaskAdded = true;
    Set<Integer> brokerInvolved = new HashSet<>();
    Set<TopicPartition> partitionsInvolved = new HashSet<>();
    while (newTaskAdded) {
      newTaskAdded = false;
      brokerInvolved.clear();
      for (Map.Entry<Integer, Integer> brokerEntry : readyBrokers.entrySet()) {
        int brokerId = brokerEntry.getKey();
        // If this broker has already involved in this round, skip it.
        if (brokerInvolved.contains(brokerId)) {
          continue;
        }

        // Check the available balancing proposals of this broker to see if we can find one ready to execute.
        SortedSet<ExecutionTask> proposalsForBroker = _interPartMoveTaskByBrokerId.get(brokerId);
        LOG.trace("Execution task for broker {} are {}", brokerId, proposalsForBroker);
        if (proposalsForBroker != null) {
          for (ExecutionTask task : proposalsForBroker) {
            // Skip this proposal if either source broker or destination broker of this proposal has already
            // involved in this round.
            int sourceBroker = task.proposal().oldLeader().brokerId();
            Set<Integer> destinationBrokers = task.proposal().replicasToAdd().stream().mapToInt(ReplicaPlacementInfo::brokerId)
                                                  .boxed().collect(Collectors.toSet());
            if (brokerInvolved.contains(sourceBroker)) {
              continue;
            }
            TopicPartition tp = task.proposal().topicPartition();
            // Check if the proposal is executable.
            if (isExecutableProposal(task.proposal(), readyBrokers)
                && !inProgressPartitions.contains(tp)
                && !partitionsInvolved.contains(tp)) {
              partitionsInvolved.add(tp);
              executableReplicaMovements.add(task);
              // Record the two brokers as involved in this round and stop involving them again in this round.
              brokerInvolved.add(sourceBroker);
              brokerInvolved.addAll(destinationBrokers);
              // Remove the proposal from the execution plan.
              removeInterBrokerReplicaActionForExecution(task);
              // Decrement the slots for both source and destination brokers
              readyBrokers.put(sourceBroker, readyBrokers.get(sourceBroker) - 1);
              for (int broker : destinationBrokers) {
                readyBrokers.put(broker, readyBrokers.get(broker) - 1);
              }
              // Mark proposal added to true so we will have another round of check.
              newTaskAdded = true;
              LOG.debug("Found ready task {} for broker {}. Broker concurrency state: {}", task, brokerId, readyBrokers);
              // We can stop the check for proposals for this broker because we have found a proposal.
              break;
            }
          }
        }
      }
    }
    return executableReplicaMovements;
  }

  /**
   * Get a list of executable intra-broker replica movements that comply with the concurrency constraint.
   *
   * @param readyBrokers The brokers that is ready to execute more movements.
   * @return A list of movements that is executable for the ready brokers.
   */
  public List<ExecutionTask> getIntraBrokerReplicaMovementTasks(Map<Integer, Integer> readyBrokers) {
    LOG.trace("Getting intra-broker replica movement tasks for brokers with concurrency {}", readyBrokers);
    List<ExecutionTask> executableReplicaMovements = new ArrayList<>();

    for (Map.Entry<Integer, Integer> brokerEntry : readyBrokers.entrySet()) {
      int brokerId = brokerEntry.getKey();
      int limit = brokerEntry.getValue();
      if (_intraPartMoveTaskByBrokerId.containsKey(brokerId)) {
        Iterator<ExecutionTask> tasksForBroker = _intraPartMoveTaskByBrokerId.get(brokerId).iterator();
        while (limit-- > 0 && tasksForBroker.hasNext()) {
          ExecutionTask task =  tasksForBroker.next();
          executableReplicaMovements.add(task);
          // Remove the proposal from the execution plan.
          tasksForBroker.remove();
          _remainingIntraBrokerReplicaMovements.remove(task);
        }
      }
    }
    return executableReplicaMovements;
  }

  /**
   * Clear all the states.
   */
  public void clear() {
    _intraPartMoveTaskByBrokerId.clear();
    _interPartMoveTaskByBrokerId.clear();
    _remainingLeadershipMovements.clear();
    _remainingInterBrokerReplicaMovements.clear();
    _remainingIntraBrokerReplicaMovements.clear();
  }

  /**
   * A proposal is executable if the source broker and all the destination brokers are ready. i.e. has slots to
   * execute more proposals.
   */
  private boolean isExecutableProposal(ExecutionProposal proposal, Map<Integer, Integer> readyBrokers) {
    if (readyBrokers.get(proposal.oldLeader().brokerId()) <= 0) {
      return false;
    }
    for (ReplicaPlacementInfo destinationBroker : proposal.replicasToAdd()) {
      if (readyBrokers.get(destinationBroker.brokerId()) <= 0) {
        return false;
      }
    }
    return true;
  }

  private void removeInterBrokerReplicaActionForExecution(ExecutionTask task) {
    int sourceBroker = task.proposal().oldLeader().brokerId();
    _interPartMoveTaskByBrokerId.get(sourceBroker).remove(task);
    for (ReplicaPlacementInfo destinationBroker : task.proposal().replicasToAdd()) {
      _interPartMoveTaskByBrokerId.get(destinationBroker.brokerId()).remove(task);
    }
    _remainingInterBrokerReplicaMovements.remove(task);
  }
}
