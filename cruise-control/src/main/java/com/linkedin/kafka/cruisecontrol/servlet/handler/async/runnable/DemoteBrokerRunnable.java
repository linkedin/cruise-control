/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ensureDisjoint;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckCapacityEstimation;
import static com.linkedin.kafka.cruisecontrol.model.Disk.State.DEMOTED;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DRYRUN;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_SKIP_URP_DEMOTION;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXCLUDE_FOLLOWER_DEMOTION;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_STOP_ONGOING_EXECUTION;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;


/**
 * The async runnable for broker demotion.
 */
public class DemoteBrokerRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(DemoteBrokerRunnable.class);
  protected final Set<Integer> _brokerIds;
  protected final boolean _dryRun;
  protected final boolean _allowCapacityEstimation;
  protected final Integer _concurrentLeaderMovements;
  protected final Long _executionProgressCheckIntervalMs;
  protected final boolean _skipUrpDemotion;
  protected final boolean _excludeFollowerDemotion;
  protected final Long _replicationThrottle;
  protected final String _uuid;
  protected final String _reason;
  protected final boolean _excludeRecentlyDemotedBrokers;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Map<Integer, Set<String>> _brokerIdAndLogdirs;
  protected final boolean _isTriggeredByUserRequest;
  protected final boolean _stopOngoingExecution;

  /**
   * Constructor to be used for creating a runnable for self-healing.
   */
  public DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                              Set<Integer> demotedBrokerIds,
                              boolean allowCapacityEstimation,
                              boolean excludeRecentlyDemotedBrokers,
                              String anomalyId,
                              String reason) {
    super(kafkaCruiseControl, new OperationFuture("Slow Broker Self-Healing"));
    _brokerIds = demotedBrokerIds;
    _dryRun = SELF_HEALING_DRYRUN;
    _allowCapacityEstimation = allowCapacityEstimation;
    _concurrentLeaderMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _skipUrpDemotion = SELF_HEALING_SKIP_URP_DEMOTION;
    _excludeFollowerDemotion = SELF_HEALING_EXCLUDE_FOLLOWER_DEMOTION;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    _uuid = anomalyId;
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _brokerIdAndLogdirs = Collections.emptyMap();
    _reason = reason;
    _isTriggeredByUserRequest = false;
    _stopOngoingExecution = SELF_HEALING_STOP_ONGOING_EXECUTION;
  }

  public DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                              OperationFuture future,
                              String uuid,
                              DemoteBrokerParameters parameters) {
    super(kafkaCruiseControl, future);
    _brokerIds = parameters.brokerIds();
    _dryRun = parameters.dryRun();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _skipUrpDemotion = parameters.skipUrpDemotion();
    _excludeFollowerDemotion = parameters.excludeFollowerDemotion();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _uuid = uuid;
    _reason = parameters.reason();
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _brokerIdAndLogdirs = parameters.brokerIdAndLogdirs();
    _isTriggeredByUserRequest = true;
    _stopOngoingExecution = parameters.stopOngoingExecution();
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(demoteBrokers(), _kafkaCruiseControl.config());
  }

  /**
   * Demote given brokers by making all the replicas on these brokers the least preferred replicas for leadership election
   * within their corresponding partitions and then triggering a preferred leader election on the partitions to migrate
   * the leader replicas off the brokers.
   *
   * The result of the broker demotion is not guaranteed to be able to move all the leaders away from the
   * given brokers. The operation is with best effort. There are various possibilities that some leaders
   * cannot be migrated (e.g. no other broker is in the ISR).
   *
   * Also, this method is stateless, i.e. a demoted broker will not remain in a demoted state after this
   * operation. If there is another broker failure, the leader may be moved to the demoted broker again
   * by Kafka controller.
   *
   * @return The optimization result.
   * @throws KafkaCruiseControlException When any exception occurred during the broker demotion.
   */
  public OptimizerResult demoteBrokers() throws KafkaCruiseControlException {
    _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(_skipUrpDemotion,
                                                                       _excludeFollowerDemotion,
                                                                       _skipUrpDemotion ? _kafkaCruiseControl.kafkaCluster() : null);
    OperationProgress operationProgress = _future.operationProgress();
    if (_stopOngoingExecution) {
      maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, operationProgress);
    }
    try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(operationProgress)) {
      ensureDisjoint(_brokerIds, _brokerIdAndLogdirs.keySet(),
                     "Attempt to demote the broker and its disk in the same request is not allowed.");
      Set<Integer> brokersToCheckPresence = new HashSet<>(_brokerIds);
      brokersToCheckPresence.addAll(_brokerIdAndLogdirs.keySet());
      _kafkaCruiseControl.sanityCheckBrokerPresence(brokersToCheckPresence);
      ClusterModel clusterModel = _brokerIdAndLogdirs.isEmpty() ? _kafkaCruiseControl.clusterModel(goal.clusterModelCompletenessRequirements(),
                                                                                                   operationProgress)
                                                                : _kafkaCruiseControl.clusterModel(DEFAULT_START_TIME_FOR_CLUSTER_MODEL,
                                                                                                   _kafkaCruiseControl.timeMs(),
                                                                                                   goal.clusterModelCompletenessRequirements(),
                                                                                                   true,
                                                                                                   operationProgress);
      _brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEMOTED));
      _brokerIdAndLogdirs.forEach((brokerid, logdirs) -> {
        Broker broker = clusterModel.broker(brokerid);
        for (String logdir : logdirs) {
          if (broker.disk(logdir) == null) {
            throw new IllegalStateException(String.format("Broker %d does not have logdir %s.", brokerid, logdir));
          }
          broker.disk(logdir).setState(DEMOTED);
        }
      });
      List<Goal> goalsByPriority = goalsByPriority(Collections.singletonList(goal.getClass().getSimpleName()),
                                                   _kafkaCruiseControl.config());
      if (goalsByPriority.isEmpty()) {
        throw new IllegalArgumentException("At least one goal must be provided to get an optimization result.");
      } else if (!clusterModel.isClusterAlive()) {
        throw new IllegalArgumentException("All brokers are dead in the cluster.");
      }
      sanityCheckCapacityEstimation(_allowCapacityEstimation, clusterModel.capacityEstimationInfoByBrokerId());
      ExecutorState executorState = _kafkaCruiseControl.executorState();
      Set<Integer> excludedBrokersForLeadership = _excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                                : Collections.emptySet();

      Set<String> excludedTopics = _kafkaCruiseControl.excludedTopics(clusterModel, null);
      LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
      OptimizationOptions optimizationOptions = new OptimizationOptions(excludedTopics,
                                                                        excludedBrokersForLeadership,
                                                                        Collections.emptySet(),
                                                                        false,
                                                                        Collections.emptySet(),
                                                                        false);

      OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, goalsByPriority, operationProgress, null, optimizationOptions);
      if (!_dryRun) {
        _kafkaCruiseControl.executeDemotion(result.goalProposals(), _brokerIds, _concurrentLeaderMovements, clusterModel.brokers().size(),
                                            _executionProgressCheckIntervalMs, _replicaMovementStrategy, _replicationThrottle, _isTriggeredByUserRequest,
                                            _uuid, _reason);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }
}
