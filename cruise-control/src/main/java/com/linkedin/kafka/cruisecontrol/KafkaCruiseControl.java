/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerState;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorState;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitorState;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.partitionWithOfflineReplicas;


/**
 * The main class of Cruise Control.
 */
public class KafkaCruiseControl {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControl.class);
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
    _anomalyDetector = new AnomalyDetector(this, _time, dropwizardMetricRegistry);
    long demotionHistoryRetentionTimeMs = config.getLong(KafkaCruiseControlConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG);
    long removalHistoryRetentionTimeMs = config.getLong(KafkaCruiseControlConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG);
    _executor = new Executor(config, _time, dropwizardMetricRegistry, demotionHistoryRetentionTimeMs,
                             removalHistoryRetentionTimeMs, _anomalyDetector);
    _loadMonitor = new LoadMonitor(config, _time, _executor, dropwizardMetricRegistry, KafkaMetricDef.commonMetricDef());
    _goalOptimizerExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("GoalOptimizerExecutor", true, null));
    _goalOptimizer = new GoalOptimizer(config, _loadMonitor, _time, dropwizardMetricRegistry, _executor);
  }

  /**
   * @return The load monitor.
   */
  public LoadMonitor loadMonitor() {
    return _loadMonitor;
  }
  /**
   * Refresh the cluster metadata and get the corresponding cluster and generation information.
   *
   * @return Cluster and generation information after refreshing the cluster metadata.
   */
  public MetadataClient.ClusterAndGeneration refreshClusterAndGeneration() {
    return _loadMonitor.refreshClusterAndGeneration();
  }

  /**
   * @return The state of load monitor's task runner.
   */
  public LoadMonitorTaskRunner.LoadMonitorTaskRunnerState getLoadMonitorTaskRunnerState() {
    return _loadMonitor.taskRunnerState();
  }

  /**
   * Acquire the semaphore for the cluster model generation.
   *
   * @param operationProgress the progress for the job.
   * @return A new auto closeable semaphore for the cluster model generation.
   */
  public LoadMonitor.AutoCloseableSemaphore acquireForModelGeneration(OperationProgress operationProgress)
      throws InterruptedException {
    return _loadMonitor.acquireForModelGeneration(operationProgress);
  }

  /**
   * @return The current time in milliseconds.
   */
  public long timeMs() {
    return _time.milliseconds();
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

  /**
   * Shutdown Cruise Control.
   */
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
   * Allow a reference to {@link UserTaskManager} to be passed to {@link Executor}
   * @param userTaskManager a reference to {@link UserTaskManager}
   */
  public void setUserTaskManagerInExecutor(UserTaskManager userTaskManager) {
    _executor.setUserTaskManager(userTaskManager);
  }

  /**
   * Sanity check that if current request is not a dryrun, there is
   * (1) no ongoing execution in current Cruise Control deployment.
   * (2) no ongoing partition reassignment, which could be triggered by other admin tools or previous Cruise Control deployment.
   * This method helps to fail fast if a user attempts to start an execution during an ongoing admin operation.
   *
   * @param dryRun True if the request is just a dryrun, false if the intention is to start an execution.
   */
  public void sanityCheckDryRun(boolean dryRun) {
    if (dryRun) {
      return;
    }
    if (_executor.hasOngoingExecution()) {
      throw new IllegalStateException("Cannot execute new proposals while there is an ongoing execution.");
    }
    if (_executor.hasOngoingPartitionReassignments()) {
      throw new IllegalStateException("Cannot execute new proposals while there are ongoing partition reassignments.");
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
   * Get the cluster model cutting off at the current timestamp.
   * @param requirements the model completeness requirements.
   * @param operationProgress the progress of the job to report.
   * @return The cluster workload model.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   */
  public ClusterModel clusterModel(ModelCompletenessRequirements requirements, OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    return _loadMonitor.clusterModel(timeMs(), requirements, operationProgress);
  }

  /**
   * Get the cluster model for a given time window.
   * @param from the start time of the window
   * @param to the end time of the window
   * @param requirements the load completeness requirements.
   * @param populateReplicaPlacementInfo whether populate replica placement information.
   * @param operationProgress the progress of the job to report.
   * @return The cluster workload model.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   ModelCompletenessRequirements requirements,
                                   boolean populateReplicaPlacementInfo,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    return _loadMonitor.clusterModel(from, to, requirements, populateReplicaPlacementInfo, operationProgress);
  }

  /**
   * Bootstrap the load monitor for a given period.
   *
   * @param startMs the starting time of the bootstrap period, or null if no time period will be used.
   * @param endMs the end time of the bootstrap period, or null if no end time is specified.
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(Long startMs, Long endMs, boolean clearMetrics) {
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
  }

  /**
   * Pause all the activities of the load monitor. The load monitor can only be paused when it is in
   * RUNNING state.
   *
   * @param reason The reason for pausing metric sampling.
   */
  public void pauseMetricSampling(String reason) {
    _loadMonitor.pauseMetricSampling(reason);
  }

  /**
   * Train the load model with metric samples.
   * @param startMs training period starting time.
   * @param endMs training period end time.
   */
  public void train(Long startMs, Long endMs) {
    _loadMonitor.train(startMs, endMs);
  }

  /**
   * Enable or disable self healing for the given anomaly type in the anomaly detector.
   *
   * @param anomalyType Type of anomaly for which to enable or disable self healing.
   * @param isSelfHealingEnabled True if self healing is enabled for the given anomaly type, false otherwise.
   * @return The old value of self healing for the given anomaly type.
   */
  public boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    return _anomalyDetector.setSelfHealingFor(anomalyType, isSelfHealingEnabled);
  }

  /**
   * Drop the given brokers from the recently removed/demoted brokers.
   *
   * @param brokersToDrop Brokers to drop from the recently removed or demoted brokers.
   * @param isRemoved True to drop recently removed brokers, false to drop recently demoted brokers
   * @return {@code true} if any elements were removed from the requested set of brokers.
   */
  public boolean dropRecentBrokers(Set<Integer> brokersToDrop, boolean isRemoved) {
    return isRemoved ? _executor.dropRecentlyRemovedBrokers(brokersToDrop) : _executor.dropRecentlyDemotedBrokers(brokersToDrop);
  }

  /**
   * Add the given brokers to the recently removed/demoted brokers permanently -- i.e. until they are explicitly dropped by user.
   *
   * @param brokersToAdd Brokers to add to the recently removed or demoted brokers.
   * @param isRemoved True to add to recently removed brokers, false to add recently demoted brokers
   */
  public void addRecentBrokersPermanently(Set<Integer> brokersToAdd, boolean isRemoved) {
    if (isRemoved) {
      _executor.addRecentlyRemovedBrokers(brokersToAdd);
    } else {
      _executor.addRecentlyDemotedBrokers(brokersToAdd);
    }
  }

  /**
   * Get {@link Executor#recentlyRemovedBrokers()} if isRemoved is true, {@link Executor#recentlyDemotedBrokers()} otherwise.
   *
   * @param isRemoved True to get recently removed brokers, false to get recently demoted brokers
   * @return IDs of requested brokers.
   */
  public Set<Integer> recentBrokers(boolean isRemoved) {
    return isRemoved ? _executor.recentlyRemovedBrokers() : _executor.recentlyDemotedBrokers();
  }

  /**
   * Dynamically set the interval between checking and updating (if needed) the progress of an initiated execution.
   *
   * @param requestedExecutionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                                  execution (if null, use the default execution progress check interval
   *                                                  of Executor).
   */
  public void setRequestedExecutionProgressCheckIntervalMs(Long requestedExecutionProgressCheckIntervalMs) {
    _executor.setRequestedExecutionProgressCheckIntervalMs(requestedExecutionProgressCheckIntervalMs);
  }

  /**
   * Dynamically set the inter-broker partition movement concurrency per broker.
   *
   * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
   *                                                         per broker.
   */
  public void setRequestedInterBrokerPartitionMovementConcurrency(Integer requestedInterBrokerPartitionMovementConcurrency) {
    _executor.setRequestedInterBrokerPartitionMovementConcurrency(requestedInterBrokerPartitionMovementConcurrency);
  }

  /**
   * Dynamically set the intra-broker partition movement concurrency.
   *
   * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements.
   */
  public void setRequestedIntraBrokerPartitionMovementConcurrency(Integer requestedIntraBrokerPartitionMovementConcurrency) {
    _executor.setRequestedIntraBrokerPartitionMovementConcurrency(requestedIntraBrokerPartitionMovementConcurrency);
  }

  /**
   * Dynamically set the leadership movement concurrency.
   *
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements.
   */
  public void setRequestedLeadershipMovementConcurrency(Integer requestedLeadershipMovementConcurrency) {
    _executor.setRequestedLeadershipMovementConcurrency(requestedLeadershipMovementConcurrency);
  }

  /**
   * Resume the activities of the load monitor.
   *
   * @param reason The reason for resuming metric sampling.
   */
  public void resumeMetricSampling(String reason) {
    _loadMonitor.resumeMetricSampling(reason);
  }

  /**
   * Get the optimization proposals from the current cluster. The result would be served from the cached result if
   * it is still valid.
   * @param operationProgress the job progress to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return The optimization result.
   */
  public OptimizerResult getProposals(OperationProgress operationProgress, boolean allowCapacityEstimation)
      throws KafkaCruiseControlException {
    try {
      return _goalOptimizer.optimizations(operationProgress, allowCapacityEstimation);
    } catch (InterruptedException ie) {
      throw new KafkaCruiseControlException("Interrupted when getting the optimization proposals", ie);
    }
  }

  /**
   * Ignore the cached best proposals when:
   * <ul>
   *   <li>The caller specified goals, excluded topics, or requested to exclude brokers (e.g. recently removed brokers).</li>
   *   <li>Provided completeness requirements contain a weaker requirement than what is used by the cached proposal.</li>
   *   <li>There is an ongoing execution.</li>
   *   <li>The request is triggered by goal violation detector.</li>
   *   <li>The request involves explicitly requested destination broker Ids.</li>
   *   <li>The caller wants to rebalance across disks within the brokers.</li>
   *   <li>There are offline replicas in the cluster.</li>
   * </ul>
   * @param goals A list of goal names (i.e. each matching {@link Goal#name()}) to optimize. When empty all goals will be used.
   * @param requirements Model completeness requirements.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param excludeBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param ignoreProposalCache True to explicitly ignore the proposal cache, false otherwise.
   * @param isTriggeredByGoalViolation True if proposals is triggered by goal violation, false otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @param isRebalanceDiskMode True to generate proposal to rebalance between disks within the brokers, false otherwise.
   * @return True to ignore proposal cache, false otherwise.
   */
  public boolean ignoreProposalCache(List<String> goals,
                                     ModelCompletenessRequirements requirements,
                                     Pattern excludedTopics,
                                     boolean excludeBrokers,
                                     boolean ignoreProposalCache,
                                     boolean isTriggeredByGoalViolation,
                                     Set<Integer> requestedDestinationBrokerIds,
                                     boolean isRebalanceDiskMode) {
    ModelCompletenessRequirements requirementsForCache = _goalOptimizer.modelCompletenessRequirementsForPrecomputing();
    boolean hasWeakerRequirement =
        requirementsForCache.minMonitoredPartitionsPercentage() > requirements.minMonitoredPartitionsPercentage()
        || requirementsForCache.minRequiredNumWindows() > requirements.minRequiredNumWindows()
        || (requirementsForCache.includeAllTopics() && !requirements.includeAllTopics());

    return _executor.hasOngoingExecution() || ignoreProposalCache || (goals != null && !goals.isEmpty())
           || hasWeakerRequirement || excludedTopics != null || excludeBrokers || isTriggeredByGoalViolation
           || !requestedDestinationBrokerIds.isEmpty() || isRebalanceDiskMode
           || partitionWithOfflineReplicas(kafkaCluster()) != null;
  }

  /**
   * See {@link GoalOptimizer#optimizations(ClusterModel, List, OperationProgress, Map, OptimizationOptions)}.
   * @return Results of optimization containing the proposals and stats.
   */
  public synchronized OptimizerResult optimizations(ClusterModel clusterModel,
                                                    List<Goal> goalsByPriority,
                                                    OperationProgress operationProgress,
                                                    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution,
                                                    OptimizationOptions optimizationOptions)
      throws KafkaCruiseControlException {
    return _goalOptimizer.optimizations(clusterModel, goalsByPriority, operationProgress, initReplicaDistribution, optimizationOptions);
  }

  /**
   * See {@link GoalOptimizer#excludedTopics(ClusterModel, Pattern)}.
   * @return Set of excluded topics in the given cluster model.
   */
  public Set<String> excludedTopics(ClusterModel clusterModel, Pattern requestedExcludedTopics) {
    return _goalOptimizer.excludedTopics(clusterModel, requestedExcludedTopics);
  }

  /**
   * @return Kafka Cruise Control config.
   */
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
   * @param concurrentIntraBrokerPartitionMovements The maximum number of concurrent intra-broker partition movements
   *                                                (if null, use num.concurrent.intra.broker.partition.movements).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param executionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                         execution (if null, use execution.progress.check.interval.ms).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing proposals (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   * @param reason Reason of the execution.
   */
  public void executeProposals(Set<ExecutionProposal> proposals,
                               Set<Integer> unthrottledBrokers,
                               boolean isKafkaAssignerMode,
                               Integer concurrentInterBrokerPartitionMovements,
                               Integer concurrentIntraBrokerPartitionMovements,
                               Integer concurrentLeaderMovements,
                               Long executionProgressCheckIntervalMs,
                               ReplicaMovementStrategy replicaMovementStrategy,
                               Long replicationThrottle,
                               boolean isTriggeredByUserRequest,
                               String uuid,
                               String reason) {
    if (hasProposalsToExecute(proposals, uuid)) {
      // Set the execution mode, add execution proposals, and start execution.
      _executor.setExecutionMode(isKafkaAssignerMode);
      _executor.executeProposals(proposals, unthrottledBrokers, null, _loadMonitor,
                                 concurrentInterBrokerPartitionMovements, concurrentIntraBrokerPartitionMovements,
                                 concurrentLeaderMovements, executionProgressCheckIntervalMs, replicaMovementStrategy,
                                 replicationThrottle, isTriggeredByUserRequest, uuid, reason);
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
   * @param executionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                         execution (if null, use execution.progress.check.interval.ms).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing remove operations (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   * @param reason Reason of the execution.
   */
  public void executeRemoval(Set<ExecutionProposal> proposals,
                             boolean throttleDecommissionedBroker,
                             Set<Integer> removedBrokers,
                             boolean isKafkaAssignerMode,
                             Integer concurrentInterBrokerPartitionMovements,
                             Integer concurrentLeaderMovements,
                             Long executionProgressCheckIntervalMs,
                             ReplicaMovementStrategy replicaMovementStrategy,
                             Long replicationThrottle,
                             boolean isTriggeredByUserRequest,
                             String uuid,
                             String reason) {
    if (hasProposalsToExecute(proposals, uuid)) {
      // Set the execution mode, add execution proposals, and start execution.
      _executor.setExecutionMode(isKafkaAssignerMode);
      _executor.executeProposals(proposals, throttleDecommissionedBroker ? Collections.emptySet() : removedBrokers,
                                 removedBrokers, _loadMonitor, concurrentInterBrokerPartitionMovements, 0,
                                 concurrentLeaderMovements, executionProgressCheckIntervalMs, replicaMovementStrategy,
                                 replicationThrottle, isTriggeredByUserRequest, uuid, reason);
    }
  }

  /**
   * Execute the given balancing proposals for demote operations.
   * @param proposals The given balancing proposals
   * @param demotedBrokers Brokers to be demoted.
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param brokerCount Number of brokers in the cluster.
   * @param executionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                         execution (if null, use execution.progress.check.interval.ms).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing demote operations (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   * @param reason Reason of the execution.
   */
  public void executeDemotion(Set<ExecutionProposal> proposals,
                              Set<Integer> demotedBrokers,
                              Integer concurrentLeaderMovements,
                              int brokerCount,
                              Long executionProgressCheckIntervalMs,
                              ReplicaMovementStrategy replicaMovementStrategy,
                              Long replicationThrottle,
                              boolean isTriggeredByUserRequest,
                              String uuid,
                              String reason) {
    if (hasProposalsToExecute(proposals, uuid)) {
      // (1) Kafka Assigner mode is irrelevant for demoting.
      // (2) Ensure that replica swaps within partitions, which are prerequisites for broker demotion and does not trigger data move,
      //     are throttled by concurrentLeaderMovements and config max.num.cluster.movements.
      int concurrentSwaps = concurrentLeaderMovements != null
                            ? concurrentLeaderMovements
                            : _config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG);
      concurrentSwaps = Math.min(_config.getInt(KafkaCruiseControlConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG) / brokerCount, concurrentSwaps);

      // Set the execution mode, add execution proposals, and start execution.
      _executor.setExecutionMode(false);
      _executor.executeDemoteProposals(proposals, demotedBrokers, _loadMonitor, concurrentSwaps, concurrentLeaderMovements,
                                       executionProgressCheckIntervalMs, replicaMovementStrategy, replicationThrottle,
                                       isTriggeredByUserRequest, uuid, reason);
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   *
   * @param forceExecutionStop Whether force execution to stop.
   */
  public void userTriggeredStopExecution(boolean forceExecutionStop) {
    _executor.userTriggeredStopExecution(forceExecutionStop);
  }

  /**
   * @return The current state of the executor.
   */
  public ExecutorState.State executionState() {
    return executorState().state();
  }

  /**
   * @return Executor state.
   */
  public ExecutorState executorState() {
    return _executor.state();
  }

  /**
   * Get the state of the load monitor.
   *
   * @param operationProgress The progress for the job.
   * @param clusterAndGeneration The current cluster and generation.
   * @return The state of the load monitor.
   */
  public LoadMonitorState monitorState(OperationProgress operationProgress,
                                       MetadataClient.ClusterAndGeneration clusterAndGeneration) {
    return _loadMonitor.state(operationProgress, clusterAndGeneration);
  }

  /**
   * Get the analyzer state from the goal optimizer.
   *
   * @param clusterAndGeneration The current cluster and generation.
   * @return The analyzer state.
   */
  public AnalyzerState analyzerState(MetadataClient.ClusterAndGeneration clusterAndGeneration) {
    return _goalOptimizer.state(clusterAndGeneration);
  }

  /**
   * @return Anomaly detector state.
   */
  public AnomalyDetectorState anomalyDetectorState() {
    return _anomalyDetector.anomalyDetectorState();
  }

  /**
   * @return The cluster information from Kafka metadata.
   */
  public Cluster kafkaCluster() {
    return _loadMonitor.kafkaCluster();
  }

  /**
   * @return The topic config provider.
   */
  public TopicConfigProvider topicConfigProvider() {
    return _loadMonitor.topicConfigProvider();
  }

  /**
   * @return The Kafka Cruise Control Version
   */
  public static String cruiseControlVersion() {
    return VERSION;
  }

  /**
   * @return The Kafka Cruise Control's current code's commit id
   */
  public static String cruiseControlCommitId() {
    return COMMIT_ID;
  }

  /**
   * If the goals is empty or null, return the default model completeness requirements, otherwise combine the load
   * requirement options for the given goals and return the resulting model completeness requirements.
   *
   * @param goals Goals to combine load requirement options.
   * @return Model completeness requirements.
   */
  public ModelCompletenessRequirements modelCompletenessRequirements(Collection<Goal> goals) {
    return goals == null || goals.isEmpty() ?
           _goalOptimizer.defaultModelCompletenessRequirements() : MonitorUtils.combineLoadRequirementOptions(goals);
  }

  /**
   * Check if the completeness requirements are met for the given goals.
   *
   * @param goals A list of goals to check completeness for.
   * @return True if completeness requirements are met for the given goals, false otherwise.
   */
  public boolean meetCompletenessRequirements(List<String> goals) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _loadMonitor.refreshClusterAndGeneration();
    return goalsByPriority(goals, _config).stream().allMatch(g -> _loadMonitor.meetCompletenessRequirements(
        clusterAndGeneration, g.clusterModelCompletenessRequirements()));
  }

  /**
   * Sanity check whether the provided brokers exist in cluster or not.
   * @param brokerIds A set of broker ids.
   */
  public void sanityCheckBrokerPresence(Set<Integer> brokerIds) {
    Cluster cluster = _loadMonitor.refreshClusterAndGeneration().cluster();
    Set<Integer> invalidBrokerIds = brokerIds.stream().filter(id -> cluster.nodeById(id) == null).collect(Collectors.toSet());
    if (!invalidBrokerIds.isEmpty()) {
      throw new IllegalArgumentException(String.format("Broker %s does not exist.", invalidBrokerIds));
    }
  }
}
