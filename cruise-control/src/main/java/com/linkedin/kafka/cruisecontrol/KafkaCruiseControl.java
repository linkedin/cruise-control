/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AdminParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.AdminResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.BootstrapResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaClusterState;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import com.linkedin.kafka.cruisecontrol.servlet.response.PauseSamplingResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.ResumeSamplingResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.StopProposalResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.TrainResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.*;


/**
 * The main class of Cruise Control.
 */
public class KafkaCruiseControl {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControl.class);
  private static final Set<String> KAFKA_ASSIGNER_GOALS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(),
                                                              KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName())));
  protected final KafkaCruiseControlConfig _config;
  private final LoadMonitor _loadMonitor;
  private final GoalOptimizer _goalOptimizer;
  private final ExecutorService _goalOptimizerExecutor;
  private final Executor _executor;
  private final AnomalyDetector _anomalyDetector;
  private final Time _time;

  private static final String VERSION;
  private static final String COMMIT_ID;

  // Referenced similar method to get software version in Kafka code.
  static {
    Properties props = new Properties();
    try (InputStream resourceStream = KafkaCruiseControl.class.getResourceAsStream("/cruise-control/cruise-control-version.properties")) {
      props.load(resourceStream);
    } catch (Exception e) {
      LOG.warn("Error while loading cruise-control-version.properties :" + e.getMessage());
    }
    VERSION = props.getProperty("version", "unknown").trim();
    COMMIT_ID = props.getProperty("commitId", "unknown").trim();
    LOG.info("COMMIT INFO: " + VERSION + "---" + COMMIT_ID);
  }
  /**
   * Construct the Cruise Control
   *
   * @param config the configuration of Cruise Control.
   */
  public KafkaCruiseControl(KafkaCruiseControlConfig config, MetricRegistry dropwizardMetricRegistry) {
    _config = config;
    _time = new SystemTime();
    // initialize some of the static state of Kafka Cruise Control;
    ModelUtils.init(config);
    ModelParameters.init(config);

    // Instantiate the components.
    _loadMonitor = new LoadMonitor(config, _time, dropwizardMetricRegistry, KafkaMetricDef.commonMetricDef());
    _goalOptimizerExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("GoalOptimizerExecutor", true, null));
    long demotionHistoryRetentionTimeMs = config.getLong(KafkaCruiseControlConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG);
    long removalHistoryRetentionTimeMs = config.getLong(KafkaCruiseControlConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG);
    _executor = new Executor(config, _time, dropwizardMetricRegistry, demotionHistoryRetentionTimeMs, removalHistoryRetentionTimeMs);
    _goalOptimizer = new GoalOptimizer(config, _loadMonitor, _time, dropwizardMetricRegistry, _executor);
    _anomalyDetector = new AnomalyDetector(config, _loadMonitor, this, _time, dropwizardMetricRegistry);
  }

  /**
   * Start up the Cruise Control.
   */
  public void startUp() {
    LOG.info("Starting Kafka Cruise Control...");
    _loadMonitor.startUp();
    _anomalyDetector.startDetection();
    _goalOptimizerExecutor.submit(_goalOptimizer);
    LOG.info("Kafka Cruise Control started.");
  }

  public void shutdown() {
    Thread t = new Thread() {
      @Override
      public void run() {
        LOG.info("Shutting down Kafka Cruise Control...");
        _loadMonitor.shutdown();
        _executor.shutdown();
        _anomalyDetector.shutdown();
        _goalOptimizer.shutdown();
        LOG.info("Kafka Cruise Control shutdown completed.");
      }
    };
    t.setDaemon(true);
    t.start();
    try {
      t.join(30000);
    } catch (InterruptedException e) {
      LOG.warn("Cruise Control failed to shutdown in 30 seconds. Exit.");
    }
  }

  /**
   * Check whether any of the given goals contain a Kafka Assigner goal.
   *
   * @param goals The goals to check
   * @return True if the given goals contain a Kafka Assigner goal, false otherwise.
   */
  private boolean isKafkaAssignerMode(Collection<String> goals) {
    return goals.stream().anyMatch(KAFKA_ASSIGNER_GOALS::contains);
  }

  /**
   * Allow a reference to {@link UserTaskManager} to be passed to {@link Executor}
   * @param userTaskManager a reference to {@link UserTaskManager}
   */
  public void setUserTaskManagerInExecutor(UserTaskManager userTaskManager) {
    _executor.setUserTaskManager(userTaskManager);
  }

  /**
   * Decommission a broker.
   *
   * @param removedBrokers The brokers to decommission.
   * @param dryRun Whether it is a dry run or not.
   * @param throttleDecommissionedBroker Whether throttle the brokers that are being decommissioned.
   * @param goals The goals to be met when decommissioning the brokers. When empty all goals will be used.
   * @param requirements The cluster model completeness requirements.
   * @param operationProgress The progress to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param skipHardGoalCheck True if the provided {@code goals} do not have to contain all hard goals, false otherwise.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param uuid UUID of the execution.
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @return The optimization result.
   *
   * @throws KafkaCruiseControlException when any exception occurred during the decommission process.
   */
  public GoalOptimizer.OptimizerResult decommissionBrokers(Set<Integer> removedBrokers,
                                                           boolean dryRun,
                                                           boolean throttleDecommissionedBroker,
                                                           List<String> goals,
                                                           ModelCompletenessRequirements requirements,
                                                           OperationProgress operationProgress,
                                                           boolean allowCapacityEstimation,
                                                           Integer concurrentInterBrokerPartitionMovements,
                                                           Integer concurrentLeaderMovements,
                                                           boolean skipHardGoalCheck,
                                                           Pattern excludedTopics,
                                                           ReplicaMovementStrategy replicaMovementStrategy,
                                                           String uuid,
                                                           boolean excludeRecentlyDemotedBrokers,
                                                           boolean excludeRecentlyRemovedBrokers,
                                                           Set<Integer> requestedDestinationBrokerIds)
      throws KafkaCruiseControlException {
    sanityCheckDryRun(dryRun);
    sanityCheckHardGoalPresence(goals, skipHardGoalCheck);
    List<Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements modelCompletenessRequirements =
        modelCompletenessRequirements(goalsByPriority).weaker(requirements);
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(), modelCompletenessRequirements,
                                                            operationProgress);
      removedBrokers.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
      GoalOptimizer.OptimizerResult result = getProposals(clusterModel,
                                                          goalsByPriority,
                                                          operationProgress,
                                                          allowCapacityEstimation,
                                                          excludedTopics,
                                                          excludeRecentlyDemotedBrokers,
                                                          excludeRecentlyRemovedBrokers,
                                                          false,
                                                          requestedDestinationBrokerIds);
      if (!dryRun) {
        executeRemoval(result.goalProposals(), throttleDecommissionedBroker, removedBrokers, isKafkaAssignerMode(goals),
                       concurrentInterBrokerPartitionMovements, concurrentLeaderMovements, replicaMovementStrategy, uuid);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Check whether the given capacity estimation info indicates estimations for any broker when capacity estimation is
   * not permitted.
   *
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param capacityEstimationInfoByBrokerId Capacity estimation info by broker id for which there has been an estimation.
   */
  public static void sanityCheckCapacityEstimation(boolean allowCapacityEstimation,
                                                   Map<Integer, String> capacityEstimationInfoByBrokerId) {
    if (!(allowCapacityEstimation || capacityEstimationInfoByBrokerId.isEmpty())) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("Allow capacity estimation or fix dependencies to capture broker capacities.%n"));
      for (Map.Entry<Integer, String> entry : capacityEstimationInfoByBrokerId.entrySet()) {
        sb.append(String.format("Broker: %d: info: %s%n", entry.getKey(), entry.getValue()));
      }
      throw new IllegalStateException(sb.toString());
    }
  }

  /**
   * Add brokers
   * @param brokerIds The broker ids.
   * @param dryRun Whether it is a dry run or not.
   * @param throttleAddedBrokers Whether throttle the brokers that are being added.
   * @param goals The goals to be met when adding the brokers. When empty all goals will be used.
   * @param requirements The cluster model completeness requirements.
   * @param operationProgress The progress of the job to update.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param skipHardGoalCheck True if the provided {@code goals} do not have to contain all hard goals, false otherwise.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param uuid UUID of the execution.
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @return The optimization result.
   * @throws KafkaCruiseControlException When any exception occurred during the broker addition.
   */
  public GoalOptimizer.OptimizerResult addBrokers(Set<Integer> brokerIds,
                                                  boolean dryRun,
                                                  boolean throttleAddedBrokers,
                                                  List<String> goals,
                                                  ModelCompletenessRequirements requirements,
                                                  OperationProgress operationProgress,
                                                  boolean allowCapacityEstimation,
                                                  Integer concurrentInterBrokerPartitionMovements,
                                                  Integer concurrentLeaderMovements,
                                                  boolean skipHardGoalCheck,
                                                  Pattern excludedTopics,
                                                  ReplicaMovementStrategy replicaMovementStrategy,
                                                  String uuid,
                                                  boolean excludeRecentlyDemotedBrokers,
                                                  boolean excludeRecentlyRemovedBrokers) throws KafkaCruiseControlException {
    sanityCheckDryRun(dryRun);
    sanityCheckHardGoalPresence(goals, skipHardGoalCheck);
    List<Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements modelCompletenessRequirements =
        modelCompletenessRequirements(goalsByPriority).weaker(requirements);
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      sanityCheckBrokerPresence(brokerIds);
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(),
                                                            modelCompletenessRequirements,
                                                            operationProgress);
      brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.NEW));
      GoalOptimizer.OptimizerResult result = getProposals(clusterModel,
                                                          goalsByPriority,
                                                          operationProgress,
                                                          allowCapacityEstimation,
                                                          excludedTopics,
                                                          excludeRecentlyDemotedBrokers,
                                                          excludeRecentlyRemovedBrokers,
                                                          false,
                                                          Collections.emptySet());
      if (!dryRun) {
        executeProposals(result.goalProposals(),
                         throttleAddedBrokers ? Collections.emptySet() : brokerIds,
                         isKafkaAssignerMode(goals),
                         concurrentInterBrokerPartitionMovements,
                         concurrentLeaderMovements,
                         replicaMovementStrategy,
                         uuid);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Check if there is an ongoing execution and dryrun is false.
   * This method helps to fail fast if a user attempts to start an execution during an ongoing execution.
   *
   * @param dryRun True if the request is just a dryrun, false if the intention is to start an execution.
   */
  private void sanityCheckDryRun(boolean dryRun) {
    if (!dryRun && _executor.hasOngoingExecution()) {
      throw new IllegalStateException("Cannot execute new proposals while there is an ongoing execution.");
    }
  }

  /**
   * Rebalance the cluster
   * @param goals The goals to be met during the rebalance. When empty all goals will be used.
   * @param dryRun Whether it is a dry run or not.
   * @param requirements The cluster model completeness requirements.
   * @param operationProgress The progress of the job to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param skipHardGoalCheck True if the provided {@code goals} do not have to contain all hard goals, false otherwise.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param uuid UUID of the execution.
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @param ignoreProposalCache True to explicitly ignore the proposal cache, false otherwise.
   * @param isTriggeredByGoalViolation True if rebalance is triggered by goal violation, false otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @return The optimization result.
   * @throws KafkaCruiseControlException When the rebalance encounter errors.
   */
  public GoalOptimizer.OptimizerResult rebalance(List<String> goals,
                                                 boolean dryRun,
                                                 ModelCompletenessRequirements requirements,
                                                 OperationProgress operationProgress,
                                                 boolean allowCapacityEstimation,
                                                 Integer concurrentInterBrokerPartitionMovements,
                                                 Integer concurrentLeaderMovements,
                                                 boolean skipHardGoalCheck,
                                                 Pattern excludedTopics,
                                                 ReplicaMovementStrategy replicaMovementStrategy,
                                                 String uuid,
                                                 boolean excludeRecentlyDemotedBrokers,
                                                 boolean excludeRecentlyRemovedBrokers,
                                                 boolean ignoreProposalCache,
                                                 boolean isTriggeredByGoalViolation,
                                                 Set<Integer> requestedDestinationBrokerIds) throws KafkaCruiseControlException {
    sanityCheckDryRun(dryRun);
    GoalOptimizer.OptimizerResult result = getProposals(goals, requirements, operationProgress,
                                                        allowCapacityEstimation, skipHardGoalCheck,
                                                        excludedTopics, excludeRecentlyDemotedBrokers,
                                                        excludeRecentlyRemovedBrokers,
                                                        ignoreProposalCache,
                                                        isTriggeredByGoalViolation,
                                                        requestedDestinationBrokerIds);
    if (!dryRun) {
      executeProposals(result.goalProposals(), Collections.emptySet(), isKafkaAssignerMode(goals),
                       concurrentInterBrokerPartitionMovements, concurrentLeaderMovements, replicaMovementStrategy, uuid);
    }
    return result;
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
   * @param brokerIds The id of brokers to be demoted.
   * @param dryRun Whether it is a dry run or not.
   * @param operationProgress The progress of the job to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param skipUrpDemotion Whether operate on partitions which are currently under replicated.
   * @param excludeFollowerDemotion Whether operate on the partitions which only have follower replicas on the brokers.
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param uuid UUID of the execution.
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @return the optimization result.
   */
  public GoalOptimizer.OptimizerResult demoteBrokers(Set<Integer> brokerIds,
                                                     boolean dryRun,
                                                     OperationProgress operationProgress,
                                                     boolean allowCapacityEstimation,
                                                     Integer concurrentLeaderMovements,
                                                     boolean skipUrpDemotion,
                                                     boolean excludeFollowerDemotion,
                                                     ReplicaMovementStrategy replicaMovementStrategy,
                                                     String uuid,
                                                     boolean excludeRecentlyDemotedBrokers)
      throws KafkaCruiseControlException {
    sanityCheckDryRun(dryRun);
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(skipUrpDemotion,
                                                                       excludeFollowerDemotion,
                                                                       skipUrpDemotion ? _loadMonitor.kafkaCluster() : null);
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      sanityCheckBrokerPresence(brokerIds);
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(),
                                                            goal.clusterModelCompletenessRequirements(),
                                                            operationProgress);
      brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEMOTED));
      List<Goal> goalsByPriority = goalsByPriority(Collections.singletonList(goal.getClass().getSimpleName()));
      GoalOptimizer.OptimizerResult result = getProposals(clusterModel,
                                                          goalsByPriority,
                                                          operationProgress,
                                                          allowCapacityEstimation,
                                                          null,
                                                          excludeRecentlyDemotedBrokers,
                                                          false,
                                                          false,
                                                          Collections.emptySet());
      if (!dryRun) {
        executeDemotion(result.goalProposals(), brokerIds, concurrentLeaderMovements, replicaMovementStrategy, uuid);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Get the broker load stats from the cache. null will be returned if their is no cached broker load stats.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return The cached broker load statistics.
   */
  public BrokerStats cachedBrokerLoadStats(boolean allowCapacityEstimation) {
    return _loadMonitor.cachedBrokerLoadStats(allowCapacityEstimation);
  }

  /**
   * Get the cluster model cutting off at a certain timestamp.
   * @param now time.
   * @param requirements the model completeness requirements.
   * @param operationProgress the progress of the job to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return the cluster workload model.
   * @throws KafkaCruiseControlException when the cluster model generation encounter errors.
   */
  public ClusterModel clusterModel(long now,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress,
                                   boolean allowCapacityEstimation)
      throws KafkaCruiseControlException {
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      ClusterModel clusterModel = _loadMonitor.clusterModel(now, requirements, operationProgress);
      sanityCheckCapacityEstimation(allowCapacityEstimation, clusterModel.capacityEstimationInfoByBrokerId());
      return clusterModel;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Get the cluster model for a given time window.
   * @param from the start time of the window
   * @param to the end time of the window
   * @param minValidPartitionRatio the minimum valid partition ratio requirement of model
   * @param operationProgress the progress of the job to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return the cluster workload model.
   * @throws KafkaCruiseControlException when the cluster model generation encounter errors.
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   Double minValidPartitionRatio,
                                   OperationProgress operationProgress,
                                   boolean allowCapacityEstimation)
      throws KafkaCruiseControlException {
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      if (minValidPartitionRatio == null) {
        minValidPartitionRatio = _config.getDouble(KafkaCruiseControlConfig.MIN_VALID_PARTITION_RATIO_CONFIG);
      }
      ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, minValidPartitionRatio, false);
      ClusterModel clusterModel = _loadMonitor.clusterModel(from, to, requirements, operationProgress);
      sanityCheckCapacityEstimation(allowCapacityEstimation, clusterModel.capacityEstimationInfoByBrokerId());
      return clusterModel;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Bootstrap the load monitor.
   *
   * @param parameters Bootstrap parameters.
   * @return Bootstrap result.
   */
  public BootstrapResult bootstrapLoadMonitor(BootstrapParameters parameters) {
    Long startMs = parameters.startMs();
    Long endMs = parameters.endMs();
    boolean clearMetrics = parameters.clearMetrics();

    if (startMs != null && endMs != null) {
      // Bootstrap the load monitor for a given period.
      _loadMonitor.bootstrap(startMs, endMs, clearMetrics);
    } else if (startMs != null) {
      // Bootstrap the load monitor from the given timestamp until it catches up -- i.e. clears all metric samples.
      _loadMonitor.bootstrap(startMs, clearMetrics);
    } else {
      // Bootstrap the load monitor with the most recent metric samples until it catches up -- clears all metric samples.
      _loadMonitor.bootstrap(clearMetrics);
    }
    return new BootstrapResult(_config);
  }

  /**
   * Train load model of Kafka Cruise Control with metric samples in a training period.
   *
   * @param parameters Train parameters.
   * @return Train result.
   */
  public TrainResult trainLoadModel(TrainParameters parameters) {
    _loadMonitor.train(parameters.startMs(), parameters.endMs());
    return new TrainResult(_config);
  }

  /**
   * Pause all the activities of the load monitor. Load monitor can only be paused if it is in RUNNING state.
   *
   * @param parameters Pause Parameters.
   * @return Pause sampling result.
   */
  public PauseSamplingResult pauseLoadMonitorActivity(PauseResumeParameters parameters) {
    _loadMonitor.pauseMetricSampling(parameters.reason());
    return new PauseSamplingResult(_config);
  }

  /**
   * Handle the admin requests:
   * <ul>
   * <li>Dynamically change the partition and leadership concurrency of an ongoing execution. Has no effect if Executor
   * is in {@link com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State#NO_TASK_IN_PROGRESS} state.</li>
   * <li>Enable/disable the specified anomaly detectors.</li>
   * </ul>
   *
   * @param parameters Admin parameters
   * @return Admin response.
   */
  public synchronized AdminResult handleAdminRequest(AdminParameters parameters) {
    String ongoingConcurrencyChangeRequest = "";
    // 1.1. Change inter-broker partition concurrency.
    Integer concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    if (concurrentInterBrokerPartitionMovements != null) {
      _executor.setRequestedInterBrokerPartitionMovementConcurrency(concurrentInterBrokerPartitionMovements);
      ongoingConcurrencyChangeRequest += String.format("Inter-broker partition movement concurrency is set to %d%n",
                                                       concurrentInterBrokerPartitionMovements);
      LOG.warn("Inter-broker partition movement concurrency is set to: {} by user.", concurrentInterBrokerPartitionMovements);
    }
    // 1.2. Change leadership concurrency.
    Integer concurrentLeaderMovements = parameters.concurrentLeaderMovements();
    if (concurrentLeaderMovements != null) {
      _executor.setRequestedLeadershipMovementConcurrency(concurrentLeaderMovements);
      ongoingConcurrencyChangeRequest += String.format("Leadership movement concurrency is set to %d%n", concurrentLeaderMovements);
      LOG.warn("Leadership movement concurrency is set to: {} by user.", concurrentLeaderMovements);
    }

    // 2. Enable/disable the specified anomaly detectors
    Set<AnomalyType> disableSelfHealingFor = parameters.disableSelfHealingFor();
    Set<AnomalyType> enableSelfHealingFor = parameters.enableSelfHealingFor();

    Map<AnomalyType, Boolean> selfHealingBefore = new HashMap<>(disableSelfHealingFor.size() + enableSelfHealingFor.size());
    Map<AnomalyType, Boolean> selfHealingAfter = new HashMap<>(disableSelfHealingFor.size() + enableSelfHealingFor.size());

    for (AnomalyType anomalyType : disableSelfHealingFor) {
      selfHealingBefore.put(anomalyType, _anomalyDetector.setSelfHealingFor(anomalyType, false));
      selfHealingAfter.put(anomalyType, false);
    }

    for (AnomalyType anomalyType : enableSelfHealingFor) {
      selfHealingBefore.put(anomalyType, _anomalyDetector.setSelfHealingFor(anomalyType, true));
      selfHealingAfter.put(anomalyType, true);
    }

    if (!disableSelfHealingFor.isEmpty() || !enableSelfHealingFor.isEmpty()) {
      LOG.warn("Self healing state is modified by user (before: {} after: {}).", selfHealingBefore, selfHealingAfter);
    }

    return new AdminResult(selfHealingBefore,
                           selfHealingAfter,
                           ongoingConcurrencyChangeRequest.isEmpty() ? null : ongoingConcurrencyChangeRequest,
                           _config);
  }

  /**
   * Resume all the activities of the load monitor.
   *
   * @param parameters Resume Parameters.
   * @return Resume sampling result.
   */
  public ResumeSamplingResult resumeLoadMonitorActivity(PauseResumeParameters parameters) {
    _loadMonitor.resumeMetricSampling(parameters.reason());
    return new ResumeSamplingResult(_config);
  }

  /**
   * Get the optimization proposals from the current cluster. The result would be served from the cached result if
   * it is still valid.
   * @param operationProgress the job progress to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return The optimization result.
   */
  public GoalOptimizer.OptimizerResult getProposals(OperationProgress operationProgress,
                                                    boolean allowCapacityEstimation)
      throws KafkaCruiseControlException {
    try {
      return _goalOptimizer.optimizations(operationProgress, allowCapacityEstimation);
    } catch (InterruptedException ie) {
      throw new KafkaCruiseControlException("Interrupted when getting the optimization proposals", ie);
    }
  }

  /**
   * Ignore the cached best proposals when:
   * 1. The caller specified goals, excluded topics, or requested to exclude brokers (e.g. recently removed brokers).
   * 2. Provided completeness requirements contain a weaker requirement than what is used by the cached proposal.
   * 3. There is an ongoing execution.
   * 4. The request is triggered by goal violation detector.
   * 5. The request involves explicitly requested destination broker Ids.
   *
   * @param goals A list of goals to optimize. When empty all goals will be used.
   * @param requirements Model completeness requirements.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param excludeBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param ignoreProposalCache True to explicitly ignore the proposal cache, false otherwise.
   * @param isTriggeredByGoalViolation True if proposals is triggered by goal violation, false otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @return True to ignore proposal cache, false otherwise.
   */
  private boolean ignoreProposalCache(List<String> goals,
                                      ModelCompletenessRequirements requirements,
                                      Pattern excludedTopics,
                                      boolean excludeBrokers,
                                      boolean ignoreProposalCache,
                                      boolean isTriggeredByGoalViolation,
                                      Set<Integer> requestedDestinationBrokerIds) {
    ModelCompletenessRequirements requirementsForCache = _goalOptimizer.modelCompletenessRequirementsForPrecomputing();
    boolean hasWeakerRequirement =
        requirementsForCache.minMonitoredPartitionsPercentage() > requirements.minMonitoredPartitionsPercentage()
        || requirementsForCache.minRequiredNumWindows() > requirements.minRequiredNumWindows()
        || (requirementsForCache.includeAllTopics() && !requirements.includeAllTopics());

    return _executor.hasOngoingExecution() || ignoreProposalCache || (goals != null && !goals.isEmpty())
           || hasWeakerRequirement || excludedTopics != null || excludeBrokers || isTriggeredByGoalViolation
           || !requestedDestinationBrokerIds.isEmpty();
  }

  /**
   * Optimize a cluster workload model.
   * @param goals A list of goals to optimize. When empty all goals will be used.
   * @param requirements The model completeness requirements to enforce when generating the proposals.
   * @param operationProgress The progress of the job to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param skipHardGoalCheck True if the provided {@code goals} do not have to contain all hard goals, false otherwise.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @param ignoreProposalCache True to explicitly ignore the proposal cache, false otherwise.
   * @param isTriggeredByGoalViolation True if proposals is triggered by goal violation, false otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @return The optimization result.
   * @throws KafkaCruiseControlException
   */
  public GoalOptimizer.OptimizerResult getProposals(List<String> goals,
                                                    ModelCompletenessRequirements requirements,
                                                    OperationProgress operationProgress,
                                                    boolean allowCapacityEstimation,
                                                    boolean skipHardGoalCheck,
                                                    Pattern excludedTopics,
                                                    boolean excludeRecentlyDemotedBrokers,
                                                    boolean excludeRecentlyRemovedBrokers,
                                                    boolean ignoreProposalCache,
                                                    boolean isTriggeredByGoalViolation,
                                                    Set<Integer> requestedDestinationBrokerIds)
      throws KafkaCruiseControlException {
    GoalOptimizer.OptimizerResult result;
    sanityCheckHardGoalPresence(goals, skipHardGoalCheck);
    List<Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements completenessRequirements = modelCompletenessRequirements(goalsByPriority).weaker(requirements);
    boolean excludeBrokers = excludeRecentlyDemotedBrokers || excludeRecentlyRemovedBrokers;
    if (ignoreProposalCache(goals,
                            completenessRequirements,
                            excludedTopics,
                            excludeBrokers,
                            ignoreProposalCache,
                            isTriggeredByGoalViolation,
                            requestedDestinationBrokerIds)) {
      try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
        ClusterModel clusterModel = _loadMonitor.clusterModel(-1,
                                                              _time.milliseconds(),
                                                              completenessRequirements,
                                                              operationProgress);
        result = getProposals(clusterModel,
                              goalsByPriority,
                              operationProgress,
                              allowCapacityEstimation,
                              excludedTopics,
                              excludeRecentlyDemotedBrokers,
                              excludeRecentlyRemovedBrokers,
                              isTriggeredByGoalViolation,
                              requestedDestinationBrokerIds);
      } catch (KafkaCruiseControlException kcce) {
        throw kcce;
      } catch (Exception e) {
        throw new KafkaCruiseControlException(e);
      }
    } else {
      result = getProposals(operationProgress, allowCapacityEstimation);
    }
    return result;
  }

  private GoalOptimizer.OptimizerResult getProposals(ClusterModel clusterModel,
                                                     List<Goal> goalsByPriority,
                                                     OperationProgress operationProgress,
                                                     boolean allowCapacityEstimation,
                                                     Pattern requestedExcludedTopics,
                                                     boolean excludeRecentlyDemotedBrokers,
                                                     boolean excludeRecentlyRemovedBrokers,
                                                     boolean isTriggeredByGoalViolation,
                                                     Set<Integer> requestedDestinationBrokerIds)
      throws KafkaCruiseControlException {
    sanityCheckCapacityEstimation(allowCapacityEstimation, clusterModel.capacityEstimationInfoByBrokerId());
    if (!requestedDestinationBrokerIds.isEmpty()) {
      sanityCheckBrokerPresence(requestedDestinationBrokerIds);
    }
    synchronized (this) {
      ExecutorState executorState = null;
      if (excludeRecentlyDemotedBrokers || excludeRecentlyRemovedBrokers) {
        executorState = state(operationProgress, Collections.singleton(EXECUTOR)).executorState();
      }

      Set<Integer> excludedBrokersForLeadership = excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                                : Collections.emptySet();

      Set<Integer> excludedBrokersForReplicaMove = excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
                                                                                 : Collections.emptySet();

      return _goalOptimizer.optimizations(clusterModel,
                                          goalsByPriority,
                                          operationProgress,
                                          requestedExcludedTopics,
                                          excludedBrokersForLeadership,
                                          excludedBrokersForReplicaMove,
                                          isTriggeredByGoalViolation,
                                          requestedDestinationBrokerIds);
    }
  }

  public KafkaCruiseControlConfig config() {
    return _config;
  }

  private static boolean hasProposalsToExecute(Collection<ExecutionProposal> proposals, String uuid) {
    if (proposals.isEmpty()) {
      LOG.info("Goals used in proposal generation for UUID {} are already satisfied.", uuid);
      return false;
    }
    return true;
  }

  /**
   * Execute the given balancing proposals for non-(demote/remove) operations.
   * @param proposals the given balancing proposals
   * @param unthrottledBrokers Brokers for which the rate of replica movements from/to will not be throttled.
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param uuid UUID of the execution.
   */
  private void executeProposals(Set<ExecutionProposal> proposals,
                                Set<Integer> unthrottledBrokers,
                                boolean isKafkaAssignerMode,
                                Integer concurrentInterBrokerPartitionMovements,
                                Integer concurrentLeaderMovements,
                                ReplicaMovementStrategy replicaMovementStrategy,
                                String uuid) {
    if (hasProposalsToExecute(proposals, uuid)) {
      // Set the execution mode, add execution proposals, and start execution.
      _executor.setExecutionMode(isKafkaAssignerMode);
      _executor.executeProposals(proposals, unthrottledBrokers, null, _loadMonitor, concurrentInterBrokerPartitionMovements,
                                 concurrentLeaderMovements, replicaMovementStrategy, uuid);
    }
  }

  /**
   * Execute the given balancing proposals for remove operations.
   * @param proposals the given balancing proposals
   * @param throttleDecommissionedBroker Whether throttle the brokers that are being decommissioned.
   * @param removedBrokers Brokers to be removed, null if no brokers has been removed.
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param uuid UUID of the execution.
   */
  private void executeRemoval(Set<ExecutionProposal> proposals,
                              boolean throttleDecommissionedBroker,
                              Set<Integer> removedBrokers,
                              boolean isKafkaAssignerMode,
                              Integer concurrentInterBrokerPartitionMovements,
                              Integer concurrentLeaderMovements,
                              ReplicaMovementStrategy replicaMovementStrategy,
                              String uuid) {
    if (hasProposalsToExecute(proposals, uuid)) {
      // Set the execution mode, add execution proposals, and start execution.
      _executor.setExecutionMode(isKafkaAssignerMode);
      _executor.executeProposals(proposals, throttleDecommissionedBroker ? Collections.emptySet() : removedBrokers,
                                 removedBrokers, _loadMonitor, concurrentInterBrokerPartitionMovements,
                                 concurrentLeaderMovements, replicaMovementStrategy, uuid);
    }
  }

  /**
   * Execute the given balancing proposals for demote operations.
   * @param proposals The given balancing proposals
   * @param demotedBrokers Brokers to be demoted.
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param uuid UUID of the execution.
   */
  private void executeDemotion(Set<ExecutionProposal> proposals,
                               Set<Integer> demotedBrokers,
                               Integer concurrentLeaderMovements,
                               ReplicaMovementStrategy replicaMovementStrategy,
                               String uuid) {
    if (hasProposalsToExecute(proposals, uuid)) {
      // (1) Kafka Assigner mode is irrelevant for demoting. (2) Ensure that replica swaps within partitions, which are
      // prerequisites for broker demotion and does not trigger data move, are throttled by concurrentLeaderMovements.
      int concurrentSwaps = concurrentLeaderMovements != null
                            ? concurrentLeaderMovements
                            : _config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG);

      // Set the execution mode, add execution proposals, and start execution.
      _executor.setExecutionMode(false);
      _executor.executeDemoteProposals(proposals, demotedBrokers, _loadMonitor, concurrentSwaps, concurrentLeaderMovements,
                                       replicaMovementStrategy, uuid);
    }
  }

  /**
   * Stop the executor if it is executing the proposals.
   *
   * @param parameters Stop proposal parameters (not used -- added for standardization and for extensibility).
   * @return Stop proposal execution result.
   */
  public StopProposalResult stopProposalExecution(StopProposalParameters parameters) {
    _executor.userTriggeredStopExecution();
    return new StopProposalResult(_config);
  }

  private void ensureNoOfflineReplicaForPartition(PartitionInfo partitionInfo, List<Node> aliveNodes) {
    for (Node node: partitionInfo.replicas()) {
      if (!aliveNodes.contains(node)) {
        throw new RuntimeException(String.format("Topic partition %s-%d have offline replica on broker %d , unable to update "
                                   + "its replication factor.", partitionInfo.topic(), partitionInfo.partition(), node.id()));
      }
    }
  }

  /**
   * Update the replication factor of Kafka topics.
   * If partition's current replication factor is less than target replication factor, add new replicas to the partition
   * in a rack-aware, round-robin way.
   * There are two scenarios that rack awareness property is not guaranteed.
   * <ul>
   *   <li> If metadata does not have rack information about brokers, then it is only guaranteed that new replicas are
   *   added to brokers which currently do not host any replicas of partition.</li>
   *   <li> If replication factor to set for the topic is larger than number of racks in the cluster and
   *   skipTopicRackAwarenessCheck is set to true, then rack awareness property is ignored.</li>
   * </ul>
   * If partition's current replication factor is larger than target replication factor, remove one or more follower replicas
   * from the partition.
   *
   * @param cluster The metadata of the cluster.
   * @param replicationFactor The replication factor to set for the topics.
   * @param topics The topics to apply the change.
   * @param skipTopicRackAwarenessCheck Whether skip ignore rack awareness property if number of rack in clsuter is less
   *                                    than target replication factor.
   * @return Execution proposals to increase replication factor of topics.
   */
  private List<ExecutionProposal> maybeUpdateTopicReplicationFactor(Cluster cluster,
                                                                    int replicationFactor,
                                                                    Set<String> topics,
                                                                    boolean skipTopicRackAwarenessCheck) {
    Map<String, List<Integer>> brokersByRack = new HashMap<>();
    Map<Integer, String> rackByBroker = new HashMap<>();
    for (Node node : cluster.nodes()) {
      // If the rack is not specified, we use the broker id info as rack info.
      String rack = node.rack() == null || node.rack().isEmpty() ? String.valueOf(node.id()) : node.rack();
      brokersByRack.putIfAbsent(rack, new ArrayList<>());
      brokersByRack.get(rack).add(node.id());
      rackByBroker.put(node.id(), rack);
    }

    if (replicationFactor > brokersByRack.size()) {
      if (skipTopicRackAwarenessCheck) {
        LOG.info(String.format("Target replication factor for topics %s is larger than number of racks in cluster, new replica"
                               + " maybe added in none rack-aware way.", topics));
      } else {
        throw new RuntimeException(String.format("Unable to increase topics %s replica factor to %d since there are only %d " +
                                                 "racks in the cluster.", topics, replicationFactor, brokersByRack.size()));
      }
    }

    List<ExecutionProposal> proposals = new ArrayList<>();
    for (String topic : topics) {
      List<String> racks = new ArrayList<>(brokersByRack.keySet());
      int[] cursors = new int[racks.size()];
      int rackCursor = 0;
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        if (partitionInfo.replicas().length < replicationFactor) {
          ensureNoOfflineReplicaForPartition(partitionInfo, cluster.nodes());
          List<Integer> currentAssignedReplica = new ArrayList<>(partitionInfo.replicas().length);
          List<Integer> newAssignedReplica = new ArrayList<>();
          Set<String> currentOccupiedRack = new HashSet<>();
          // Make sure the current replicas are in new replica list.
          for (Node node : partitionInfo.replicas()) {
            currentAssignedReplica.add(node.id());
            newAssignedReplica.add(node.id());
            currentOccupiedRack.add(rackByBroker.get(node.id()));
          }
          // Add new replica to partition in rack-aware(if possible), round-robin way.
          while (newAssignedReplica.size() < replicationFactor) {
            if (!currentOccupiedRack.contains(racks.get(rackCursor)) || currentOccupiedRack.size() == racks.size()) {
              String rack = racks.get(rackCursor);
              int cursor = cursors[rackCursor];
              newAssignedReplica.add(brokersByRack.get(rack).get(cursor));
              currentOccupiedRack.add(rack);
              cursors[rackCursor] = (cursor + 1) % brokersByRack.get(rack).size();
            }
            rackCursor = (rackCursor + 1) % racks.size();
          }
          proposals.add(new ExecutionProposal(new TopicPartition(topic, partitionInfo.partition()),
                                              0,
                                              partitionInfo.leader().id(),
                                              currentAssignedReplica,
                                              newAssignedReplica));
        } else if (partitionInfo.replicas().length > replicationFactor) {
          ensureNoOfflineReplicaForPartition(partitionInfo, cluster.nodes());
          List<Integer> currentAssignedReplica = new ArrayList<>(partitionInfo.replicas().length);
          List<Integer> newAssignedReplica = new ArrayList<>();
          // Make sure the leader replica is in new replica list.
          newAssignedReplica.add(partitionInfo.leader().id());
          for (Node node : partitionInfo.replicas()) {
            currentAssignedReplica.add(node.id());
            if (newAssignedReplica.size() < replicationFactor && node.id() != newAssignedReplica.get(0)) {
              newAssignedReplica.add(node.id());
            }
          }
          proposals.add(new ExecutionProposal(new TopicPartition(topic, partitionInfo.partition()),
                                              0,
                                              partitionInfo.leader().id(),
                                              currentAssignedReplica,
                                              newAssignedReplica));
        }
      }
    }
    return proposals;
  }

  /**
   * Update topic's configuration. Currently only support change topic's replication factor.
   *
   * @param topic The name pattern of topics to apply the change.
   * @param replicationFactor The replication factor to set for the topics.
   * @param skipTopicRackAwarenessCheck Whether skip ignore rack awareness property if number of rack in cluster is less
   *                                    than target replication factor.
   * @param uuid UUID of the execution.
   *
   * @return Topics to apply the change.
   */
  public Set<String> updateTopicConfiguration(Pattern topic,
                                              int replicationFactor,
                                              boolean skipTopicRackAwarenessCheck,
                                              String uuid
      ) {
    Cluster cluster = _loadMonitor.kafkaCluster();
    Set<String> topics = cluster.topics().stream().filter(t -> topic.matcher(t).matches()).collect(Collectors.toSet());
    List<ExecutionProposal> proposals = maybeUpdateTopicReplicationFactor(cluster,
                                                                          replicationFactor,
                                                                          topics,
                                                                          skipTopicRackAwarenessCheck);
    _executor.executeProposals(proposals, null, null, _loadMonitor, null, null, null, uuid);
    return topics;
  }

  /**
   * Get the state with selected substates for Kafka Cruise Control.
   */
  public CruiseControlState state(OperationProgress operationProgress,
                                  Set<CruiseControlState.SubState> substates) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = null;
    // In case no substate is specified, return all substates.
    substates = !substates.isEmpty() ? substates
                                     : new HashSet<>(Arrays.asList(CruiseControlState.SubState.values()));

    if (KafkaCruiseControlUtils.shouldRefreshClusterAndGeneration(substates)) {
      clusterAndGeneration = _loadMonitor.refreshClusterAndGeneration();
    }

    return new CruiseControlState(substates.contains(EXECUTOR) ? _executor.state() : null,
                                  substates.contains(MONITOR) ? _loadMonitor.state(operationProgress, clusterAndGeneration) : null,
                                  substates.contains(ANALYZER) ? _goalOptimizer.state(clusterAndGeneration) : null,
                                  substates.contains(ANOMALY_DETECTOR) ? _anomalyDetector.anomalyDetectorState() : null,
                                  _config);
  }

  /**
   * Get the cluster state for Kafka.
   *
   * @param parameters Kafka cluster state parameters (not used -- added for standardization and for extensibility).
   * @return Kafka cluster state.
   */
  public KafkaClusterState kafkaClusterState(KafkaClusterStateParameters parameters) {
    return new KafkaClusterState(_loadMonitor.kafkaCluster(), _config);
  }

  /**
   * Get the Kafka Cruise Control Version
   */
  public static String cruiseControlVersion() {
    return VERSION;
  }

  /**
   * Get the Kafka Cruise Control's current code's commit id
   */
  public static String cruiseControlCommitId() {
    return COMMIT_ID;
  }

  /**
   * Get the default model completeness requirement for Cruise Control. This is the combination of the
   * requirements of all the goals.
   */
  public ModelCompletenessRequirements defaultModelCompletenessRequirements() {
    return _goalOptimizer.defaultModelCompletenessRequirements();
  }

  private ModelCompletenessRequirements modelCompletenessRequirements(Collection<Goal> overrides) {
    return overrides == null || overrides.isEmpty() ?
           _goalOptimizer.defaultModelCompletenessRequirements() : MonitorUtils.combineLoadRequirementOptions(overrides);
  }

  /**
   * Check if the completeness requirements are met for the given goals.
   *
   * @param goals A list of goals to check completeness for.
   * @return True if completeness requirements are met for the given goals, false otherwise.
   */
  public boolean meetCompletenessRequirements(List<String> goals) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _loadMonitor.refreshClusterAndGeneration();
    return goalsByPriority(goals).stream().allMatch(g -> _loadMonitor.meetCompletenessRequirements(
        clusterAndGeneration, g.clusterModelCompletenessRequirements()));
  }

  /**
   * Get a goals by priority based on the goal list.
   *
   * @param goals A list of goals.
   * @return A list of goals sorted by highest to lowest priority.
   */
  private List<Goal> goalsByPriority(List<String> goals) {
    if (goals == null || goals.isEmpty()) {
      return AnalyzerUtils.getGoalMapByPriority(_config);
    }
    Map<String, Goal> allGoals = AnalyzerUtils.getCaseInsensitiveGoalsByName(_config);
    sanityCheckNonExistingGoal(goals, allGoals);
    return goals.stream().map(allGoals::get).collect(Collectors.toList());
  }

  /**
   * Sanity check whether all hard goals are included in provided goal list.
   * There are two special scenarios where hard goal check is skipped.
   * <ul>
   * <li> {@code goals} is null or empty -- i.e. even if hard goals are excluded from the default goals, this check will pass</li>
   * <li> {@code goals} only has PreferredLeaderElectionGoal, denotes it is a PLE request.</li>
   * </ul>
   *
   * @param goals A list of goals.
   * @param skipHardGoalCheck True if hard goal checking is not needed.
   */
  public void sanityCheckHardGoalPresence(List<String> goals, boolean skipHardGoalCheck) {
    if (goals != null && !goals.isEmpty() && !skipHardGoalCheck &&
        !(goals.size() == 1 && goals.get(0).equals(PreferredLeaderElectionGoal.class.getSimpleName()))) {
      sanityCheckNonExistingGoal(goals, AnalyzerUtils.getCaseInsensitiveGoalsByName(_config));
      Set<String> hardGoals = _config.getList(KafkaCruiseControlConfig.HARD_GOALS_CONFIG).stream()
                                     .map(goalName -> goalName.substring(goalName.lastIndexOf(".") + 1)).collect(Collectors.toSet());
      if (!goals.containsAll(hardGoals)) {
        throw new IllegalArgumentException("Missing hard goals " + hardGoals + " in the provided goals: " + goals
                                           + ". Add skip_hard_goal_check=true parameter to ignore this sanity check.");
      }
    }
  }

  /**
   * Sanity check whether the given goals exist in the given supported goals.
   * @param goals A list of goals.
   * @param supportedGoals Supported goals.
   */
  private void sanityCheckNonExistingGoal(List<String> goals, Map<String, Goal> supportedGoals) {
    Set<String> nonExistingGoals = new HashSet<>();
    goals.stream().filter(goalName -> supportedGoals.get(goalName) == null).forEach(nonExistingGoals::add);

    if (!nonExistingGoals.isEmpty()) {
      throw new IllegalArgumentException("Goals " + nonExistingGoals + " are not supported. Supported: " + supportedGoals.keySet());
    }
  }

  /**
   * Sanity check whether the provided brokers exist in cluster or not.
   * @param brokerIds A set of broker ids.
   */
  private void sanityCheckBrokerPresence(Set<Integer> brokerIds) {
    Cluster cluster = _loadMonitor.refreshClusterAndGeneration().cluster();
    Set<Integer> invalidBrokerIds = brokerIds.stream().filter(id -> cluster.nodeById(id) == null).collect(Collectors.toSet());
    if (!invalidBrokerIds.isEmpty()) {
      throw new IllegalArgumentException(String.format("Broker %s does not exist.", invalidBrokerIds));
    }
  }
}
