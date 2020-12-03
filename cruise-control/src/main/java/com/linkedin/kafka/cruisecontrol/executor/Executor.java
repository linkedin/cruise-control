/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.exception.OngoingExecutionException;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.List;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.currentUtcDate;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.OPERATION_LOGGER;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.TIME_ZONE;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.DATE_FORMAT;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorAdminUtils.*;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.SamplingMode.ALL;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.SamplingMode.BROKER_METRICS_ONLY;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * Executor for Kafka GoalOptimizer.
 * <p>
 * The executor class is responsible for talking to the Kafka cluster to execute the rebalance proposals.
 *
 * The executor is thread-safe.
 */
public class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
  private static final Logger OPERATION_LOG = LoggerFactory.getLogger(OPERATION_LOGGER);
  private static final String ZK_EXECUTOR_METRIC_GROUP = "CruiseControlExecutor";
  private static final String ZK_EXECUTOR_METRIC_TYPE = "Executor";
  // The execution progress is controlled by the ExecutionTaskManager.
  private final ExecutionTaskManager _executionTaskManager;
  private final MetadataClient _metadataClient;
  private final long _defaultExecutionProgressCheckIntervalMs;
  private Long _requestedExecutionProgressCheckIntervalMs;
  private final ExecutorService _proposalExecutor;
  private final KafkaZkClient _kafkaZkClient;
  private final AdminClient _adminClient;
  private final double _leaderMovementTimeoutMs;

  private static final int NO_STOP_EXECUTION = 0;
  private static final int STOP_EXECUTION = 1;
  private static final int FORCE_STOP_EXECUTION = 2;

  // Some state for external service to query
  private final AtomicInteger _stopSignal;
  private final Time _time;
  private volatile boolean _hasOngoingExecution;
  private volatile ExecutorState _executorState;
  private volatile String _uuid;
  private volatile Supplier<String> _reasonSupplier;
  private final ExecutorNotifier _executorNotifier;

  private final AtomicInteger _numExecutionStopped;
  private final AtomicInteger _numExecutionStoppedByUser;
  private final AtomicBoolean _executionStoppedByUser;
  private final AtomicBoolean _ongoingExecutionIsBeingModified;
  private final AtomicInteger _numExecutionStartedInKafkaAssignerMode;
  private final AtomicInteger _numExecutionStartedInNonKafkaAssignerMode;
  private volatile boolean _isKafkaAssignerMode;
  private volatile boolean _skipInterBrokerReplicaConcurrencyAdjustment;
  // TODO: Execution history is currently kept in memory, but ideally we should move it to a persistent store.
  private final long _demotionHistoryRetentionTimeMs;
  private final long _removalHistoryRetentionTimeMs;
  private final ConcurrentMap<Integer, Long> _latestDemoteStartTimeMsByBrokerId;
  private final ConcurrentMap<Integer, Long> _latestRemoveStartTimeMsByBrokerId;
  private final ScheduledExecutorService _executionHistoryScannerExecutor;
  private UserTaskManager _userTaskManager;
  private final AnomalyDetectorManager _anomalyDetectorManager;
  private final ConcurrencyAdjuster _concurrencyAdjuster;
  private final ScheduledExecutorService _concurrencyAdjusterExecutor;
  private final ConcurrentMap<ConcurrencyType, Boolean> _concurrencyAdjusterEnabled;
  private final long _minExecutionProgressCheckIntervalMs;
  public final long _slowTaskAlertingBackoffTimeMs;
  private final KafkaCruiseControlConfig _config;

  /**
   * The executor class that execute the proposals generated by optimizer.
   *
   * @param config The configurations for Cruise Control.
   */
  public Executor(KafkaCruiseControlConfig config,
                  Time time,
                  MetricRegistry dropwizardMetricRegistry,
                  AnomalyDetectorManager anomalyDetectorManager) {
    this(config, time, dropwizardMetricRegistry, null, null, anomalyDetectorManager);
  }

  /**
   * The executor class that execute the proposals generated by optimizer.
   * Package private for unit test.
   *
   * @param config The configurations for Cruise Control.
   */
  Executor(KafkaCruiseControlConfig config,
           Time time,
           MetricRegistry dropwizardMetricRegistry,
           MetadataClient metadataClient,
           ExecutorNotifier executorNotifier,
           AnomalyDetectorManager anomalyDetectorManager) {
    String zkUrl = config.getString(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG);
    _numExecutionStopped = new AtomicInteger(0);
    _numExecutionStoppedByUser = new AtomicInteger(0);
    _executionStoppedByUser = new AtomicBoolean(false);
    _ongoingExecutionIsBeingModified = new AtomicBoolean(false);
    _numExecutionStartedInKafkaAssignerMode = new AtomicInteger(0);
    _numExecutionStartedInNonKafkaAssignerMode = new AtomicInteger(0);
    _isKafkaAssignerMode = false;
    _skipInterBrokerReplicaConcurrencyAdjustment = false;
    ExecutionUtils.init(config);
    _config = config;

    _time = time;
    boolean zkSecurityEnabled = config.getBoolean(ExecutorConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    _kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zkUrl, ZK_EXECUTOR_METRIC_GROUP, ZK_EXECUTOR_METRIC_TYPE,
                                                                 zkSecurityEnabled);
    _adminClient = KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config));
    _executionTaskManager = new ExecutionTaskManager(_adminClient, dropwizardMetricRegistry, time, config);
    // Register gauge sensors.
    registerGaugeSensors(dropwizardMetricRegistry);
    _metadataClient = metadataClient != null ? metadataClient
                                             : new MetadataClient(config,
                                                                  new Metadata(ExecutionUtils.METADATA_REFRESH_BACKOFF,
                                                                               ExecutionUtils.METADATA_EXPIRY_MS,
                                                                               new LogContext(),
                                                                               new ClusterResourceListeners()),
                                                                  -1L,
                                                                  time);
    _defaultExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    _leaderMovementTimeoutMs = config.getLong(ExecutorConfig.LEADER_MOVEMENT_TIMEOUT_MS_CONFIG);
    _requestedExecutionProgressCheckIntervalMs = null;
    _proposalExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("ProposalExecutor", false, LOG));
    _latestDemoteStartTimeMsByBrokerId = new ConcurrentHashMap<>();
    _latestRemoveStartTimeMsByBrokerId = new ConcurrentHashMap<>();
    _executorState = ExecutorState.noTaskInProgress(recentlyDemotedBrokers(), recentlyRemovedBrokers());
    _stopSignal = new AtomicInteger(NO_STOP_EXECUTION);
    _hasOngoingExecution = false;
    _uuid = null;
    _reasonSupplier = null;
    _executorNotifier = executorNotifier != null ? executorNotifier
                                                 : config.getConfiguredInstance(ExecutorConfig.EXECUTOR_NOTIFIER_CLASS_CONFIG,
                                                                                ExecutorNotifier.class);
    // Must be set before execution via #setUserTaskManager(UserTaskManager)
    _userTaskManager = null;
    if (anomalyDetectorManager == null) {
      throw new IllegalStateException("Anomaly detector manager cannot be null.");
    }
    _anomalyDetectorManager = anomalyDetectorManager;
    _demotionHistoryRetentionTimeMs = config.getLong(ExecutorConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG);
    _removalHistoryRetentionTimeMs = config.getLong(ExecutorConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG);
    _minExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    _slowTaskAlertingBackoffTimeMs = config.getLong(ExecutorConfig.SLOW_TASK_ALERTING_BACKOFF_TIME_MS_CONFIG);
    _concurrencyAdjusterEnabled = new ConcurrentHashMap<>(ConcurrencyType.cachedValues().size());
    boolean allEnabled = config.getBoolean(ExecutorConfig.CONCURRENCY_ADJUSTER_ENABLED_CONFIG);
    _concurrencyAdjusterEnabled.put(ConcurrencyType.INTER_BROKER_REPLICA,
                                    allEnabled || config.getBoolean(ExecutorConfig.CONCURRENCY_ADJUSTER_INTER_BROKER_REPLICA_ENABLED_CONFIG));
    _concurrencyAdjusterEnabled.put(ConcurrencyType.LEADERSHIP,
                                    allEnabled || config.getBoolean(ExecutorConfig.CONCURRENCY_ADJUSTER_LEADERSHIP_ENABLED_CONFIG));
    // Support for intra-broker replica movement is pending https://github.com/linkedin/cruise-control/issues/1299.
    _concurrencyAdjusterEnabled.put(ConcurrencyType.INTRA_BROKER_REPLICA, false);
    _concurrencyAdjusterExecutor = Executors.newSingleThreadScheduledExecutor(
        new KafkaCruiseControlThreadFactory(ConcurrencyAdjuster.class.getSimpleName()));
    long intervalMs = config.getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_INTERVAL_MS_CONFIG);
    _concurrencyAdjuster = new ConcurrencyAdjuster();
    _concurrencyAdjusterExecutor.scheduleAtFixedRate(_concurrencyAdjuster, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    _executionHistoryScannerExecutor = Executors.newSingleThreadScheduledExecutor(
        new KafkaCruiseControlThreadFactory(ExecutionHistoryScanner.class.getSimpleName()));
    _executionHistoryScannerExecutor.scheduleAtFixedRate(new ExecutionHistoryScanner(),
                                                         ExecutionUtils.EXECUTION_HISTORY_SCANNER_INITIAL_DELAY_SECONDS,
                                                         ExecutionUtils.EXECUTION_HISTORY_SCANNER_PERIOD_SECONDS,
                                                         TimeUnit.SECONDS);
  }

  /**
   * @return The tasks that are {@link ExecutionTaskState#IN_PROGRESS} or {@link ExecutionTaskState#ABORTING} for all task types.
   */
  public Set<ExecutionTask> inExecutionTasks() {
    return _executionTaskManager.inExecutionTasks();
  }

  /**
   * Dynamically set the interval between checking and updating (if needed) the progress of an initiated execution.
   * To prevent setting this value to a very small value by mistake, ensure that the requested interval is greater than
   * the {@link #_minExecutionProgressCheckIntervalMs}.
   *
   * @param requestedExecutionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                                  execution (if null, use {@link #_defaultExecutionProgressCheckIntervalMs}).
   */
  public synchronized void setRequestedExecutionProgressCheckIntervalMs(Long requestedExecutionProgressCheckIntervalMs) {
    if (requestedExecutionProgressCheckIntervalMs != null
        && requestedExecutionProgressCheckIntervalMs < _minExecutionProgressCheckIntervalMs) {
      throw new IllegalArgumentException("Attempt to set execution progress check interval ["
                                         + requestedExecutionProgressCheckIntervalMs
                                         + "ms] to smaller than the minimum execution progress check interval in cluster ["
                                         + _minExecutionProgressCheckIntervalMs + "ms].");
    }
    _requestedExecutionProgressCheckIntervalMs = requestedExecutionProgressCheckIntervalMs;
  }

  /**
   * @return The interval between checking and updating (if needed) the progress of an initiated execution.
   */
  public long executionProgressCheckIntervalMs() {
    return _requestedExecutionProgressCheckIntervalMs == null ? _defaultExecutionProgressCheckIntervalMs
                                                              : _requestedExecutionProgressCheckIntervalMs;
  }

  /**
   * Register gauge sensors.
   */
  private void registerGaugeSensors(MetricRegistry dropwizardMetricRegistry) {
    String name = "Executor";
    dropwizardMetricRegistry.register(MetricRegistry.name(name, ExecutionUtils.GAUGE_EXECUTION_STOPPED),
                                      (Gauge<Integer>) this::numExecutionStopped);
    dropwizardMetricRegistry.register(MetricRegistry.name(name, ExecutionUtils.GAUGE_EXECUTION_STOPPED_BY_USER),
                                      (Gauge<Integer>) this::numExecutionStoppedByUser);
    dropwizardMetricRegistry.register(MetricRegistry.name(name, ExecutionUtils.GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) this::numExecutionStartedInKafkaAssignerMode);
    dropwizardMetricRegistry.register(MetricRegistry.name(name, ExecutionUtils.GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) this::numExecutionStartedInNonKafkaAssignerMode);
    dropwizardMetricRegistry.register(MetricRegistry.name(name, ExecutionUtils.GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER_CAP),
                                      (Gauge<Integer>) _executionTaskManager::interBrokerPartitionMovementConcurrency);
    dropwizardMetricRegistry.register(MetricRegistry.name(name, ExecutionUtils.GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER_CAP),
                                      (Gauge<Integer>) _executionTaskManager::intraBrokerPartitionMovementConcurrency);
    dropwizardMetricRegistry.register(MetricRegistry.name(name, ExecutionUtils.GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_GLOBAL_CAP),
                                      (Gauge<Integer>) _executionTaskManager::leadershipMovementConcurrency);
  }

  private void removeExpiredDemotionHistory() {
    LOG.debug("Remove expired demotion history");
    _latestDemoteStartTimeMsByBrokerId.entrySet().removeIf(entry -> {
      long startTime = entry.getValue();
      return startTime != ExecutionUtils.PERMANENT_TIMESTAMP && startTime + _demotionHistoryRetentionTimeMs < _time.milliseconds();
    });
  }

  private void removeExpiredRemovalHistory() {
    LOG.debug("Remove expired broker removal history");
    _latestRemoveStartTimeMsByBrokerId.entrySet().removeIf(entry -> {
      long startTime = entry.getValue();
      return startTime != ExecutionUtils.PERMANENT_TIMESTAMP && startTime + _removalHistoryRetentionTimeMs < _time.milliseconds();
    });
  }

  /**
   * A runnable class to remove expired execution history.
   */
  private class ExecutionHistoryScanner implements Runnable {
    @Override
    public void run() {
      try {
        removeExpiredDemotionHistory();
        removeExpiredRemovalHistory();
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to expire execution history.", t);
      }
    }
  }

  /**
   * A runnable class to auto-adjust the allowed reassignment concurrency for ongoing executions
   * using selected broker metrics and based on additive-increase/multiplicative-decrease (AIMD) feedback control algorithm.
   * Skips concurrency adjustment for demote operations.
   */
  private class ConcurrencyAdjuster implements Runnable {
    private LoadMonitor _loadMonitor;

    ConcurrencyAdjuster() {
      _loadMonitor = null;
    }

    /**
     * Initialize the reassignment concurrency adjustment with the load monitor and the initially requested reassignment concurrency.
     *
     * @param loadMonitor Load monitor.
     * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
     *                                                         per broker(if null, use num.concurrent.partition.movements.per.broker).
     * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements
     *                                               (if null, use num.concurrent.leader.movements).
     */
    public synchronized void initAdjustment(LoadMonitor loadMonitor,
                                            Integer requestedInterBrokerPartitionMovementConcurrency,
                                            Integer requestedLeadershipMovementConcurrency) {
      _loadMonitor = loadMonitor;
      setRequestedInterBrokerPartitionMovementConcurrency(requestedInterBrokerPartitionMovementConcurrency);
      setRequestedLeadershipMovementConcurrency(requestedLeadershipMovementConcurrency);
    }

    private boolean canRefreshConcurrency(ConcurrencyType concurrencyType) {
      if (!_concurrencyAdjusterEnabled.get(concurrencyType) || _loadMonitor == null) {
        return false;
      }
      switch (concurrencyType) {
        case LEADERSHIP:
          return _executorState.state() == LEADER_MOVEMENT_TASK_IN_PROGRESS;
        case INTER_BROKER_REPLICA:
          return _executorState.state() == ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS
                 && !_skipInterBrokerReplicaConcurrencyAdjustment;
        default:
          throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
      }
    }

    private synchronized void refreshInterBrokerReplicaConcurrency() {
      if (canRefreshConcurrency(ConcurrencyType.INTER_BROKER_REPLICA)) {
        Integer recommendedConcurrency = ExecutionUtils.recommendedConcurrency(_loadMonitor.currentBrokerMetricValues(),
                                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                                               ConcurrencyType.INTER_BROKER_REPLICA);
        if (recommendedConcurrency != null) {
          setRequestedInterBrokerPartitionMovementConcurrency(recommendedConcurrency);
        }
      }
    }

    private synchronized void refreshLeadershipConcurrency() {
      if (canRefreshConcurrency(ConcurrencyType.LEADERSHIP)) {
        Integer recommendedConcurrency = ExecutionUtils.recommendedConcurrency(_loadMonitor.currentBrokerMetricValues(),
                                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                                               ConcurrencyType.LEADERSHIP);
        if (recommendedConcurrency != null) {
          setRequestedLeadershipMovementConcurrency(recommendedConcurrency);
        }
      }
    }

    @Override
    public void run() {
      try {
        refreshInterBrokerReplicaConcurrency();
        refreshLeadershipConcurrency();
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to adjust reassignment concurrency.", t);
      }
    }
  }

  /**
   * Recently demoted brokers are the ones
   * <ul>
   *   <li>for which a broker demotion was started, regardless of how the corresponding process was completed, or</li>
   *   <li>that are explicitly marked so -- e.g. via a pluggable component using {@link #addRecentlyDemotedBrokers(Set)}.</li>
   * </ul>
   *
   * @return IDs of recently demoted brokers -- i.e. demoted within the last {@link #_demotionHistoryRetentionTimeMs}.
   */
  public Set<Integer> recentlyDemotedBrokers() {
    return Collections.unmodifiableSet(_latestDemoteStartTimeMsByBrokerId.keySet());
  }

  /**
   * Recently removed brokers are the ones
   * <ul>
   *   <li>for which a broker removal was started, regardless of how the corresponding process was completed, or</li>
   *   <li>that are explicitly marked so -- e.g. via a pluggable component using {@link #addRecentlyRemovedBrokers(Set)}.</li>
   * </ul>
   *
   * @return IDs of recently removed brokers -- i.e. removed within the last {@link #_removalHistoryRetentionTimeMs}.
   */
  public Set<Integer> recentlyRemovedBrokers() {
    return Collections.unmodifiableSet(_latestRemoveStartTimeMsByBrokerId.keySet());
  }

  /**
   * Drop the given brokers from the recently removed brokers.
   *
   * @param brokersToDrop Brokers to drop from the {@link #_latestRemoveStartTimeMsByBrokerId}.
   * @return {@code true} if any elements were removed from {@link #_latestRemoveStartTimeMsByBrokerId}.
   */
  public boolean dropRecentlyRemovedBrokers(Set<Integer> brokersToDrop) {
    return _latestRemoveStartTimeMsByBrokerId.entrySet().removeIf(entry -> (brokersToDrop.contains(entry.getKey())));
  }

  /**
   * Drop the given brokers from the recently demoted brokers.
   *
   * @param brokersToDrop Brokers to drop from the {@link #_latestDemoteStartTimeMsByBrokerId}.
   * @return {@code true} if any elements were removed from {@link #_latestDemoteStartTimeMsByBrokerId}.
   */
  public boolean dropRecentlyDemotedBrokers(Set<Integer> brokersToDrop) {
    return _latestDemoteStartTimeMsByBrokerId.entrySet().removeIf(entry -> (brokersToDrop.contains(entry.getKey())));
  }

  /**
   * Add the given brokers to the recently removed brokers permanently -- i.e. until they are explicitly dropped by user.
   * If given set has brokers that were already removed recently, make them a permanent part of recently removed brokers.
   *
   * @param brokersToAdd Brokers to add to the {@link #_latestRemoveStartTimeMsByBrokerId}.
   */
  public void addRecentlyRemovedBrokers(Set<Integer> brokersToAdd) {
    brokersToAdd.forEach(brokerId -> _latestRemoveStartTimeMsByBrokerId.put(brokerId, ExecutionUtils.PERMANENT_TIMESTAMP));
  }

  /**
   * Add the given brokers from the recently demoted brokers permanently -- i.e. until they are explicitly dropped by user.
   * If given set has brokers that were already demoted recently, make them a permanent part of recently demoted brokers.
   *
   * @param brokersToAdd Brokers to add to the {@link #_latestDemoteStartTimeMsByBrokerId}.
   */
  public void addRecentlyDemotedBrokers(Set<Integer> brokersToAdd) {
    brokersToAdd.forEach(brokerId -> _latestDemoteStartTimeMsByBrokerId.put(brokerId, ExecutionUtils.PERMANENT_TIMESTAMP));
  }

  /**
   * @return The current executor state.
   */
  public ExecutorState state() {
    return _executorState;
  }

  /**
   * Enable or disable concurrency adjuster for the given concurrency type in the executor.
   *
   * <ul>
   *   <li>TODO: Support for concurrency adjuster of {@link ConcurrencyType#INTRA_BROKER_REPLICA intra-broker replica movement}
   *   is pending <a href="https://github.com/linkedin/cruise-control/issues/1299">#1299</a>.</li>
   * </ul>
   * @param concurrencyType Type of concurrency for which to enable or disable concurrency adjuster.
   * @param isConcurrencyAdjusterEnabled {@code true} if concurrency adjuster is enabled for the given type, {@code false} otherwise.
   * @return {@code true} if concurrency adjuster was enabled before for the given concurrency type, {@code false} otherwise.
   */
  public Boolean setConcurrencyAdjusterFor(ConcurrencyType concurrencyType, boolean isConcurrencyAdjusterEnabled) {
    if (concurrencyType != ConcurrencyType.INTER_BROKER_REPLICA && concurrencyType != ConcurrencyType.LEADERSHIP) {
      throw new IllegalArgumentException(String.format("Concurrency adjuster for %s is not yet supported.", concurrencyType));
    }
    return _concurrencyAdjusterEnabled.put(concurrencyType, isConcurrencyAdjusterEnabled);
  }

  /**
   * Initialize proposal execution and start execution.
   *
   * @param proposals Proposals to be executed.
   * @param unthrottledBrokers Brokers that are not throttled in terms of the number of in/out replica movements.
   * @param removedBrokers Removed brokers, null if no brokers has been removed.
   * @param loadMonitor Load monitor.
   * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
   *                                                         per broker(if null, use num.concurrent.partition.movements.per.broker).
   * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements
   *                                                         (if null, use num.concurrent.intra.broker.partition.movements).
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements
   *                                               (if null, use num.concurrent.leader.movements).
   * @param requestedExecutionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                                  execution (if null, use execution.progress.check.interval.ms).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing a proposal (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   * @param isKafkaAssignerMode {@code true} if kafka assigner mode, {@code false} otherwise.
   * @param skipInterBrokerReplicaConcurrencyAdjustment {@code true} to skip auto adjusting concurrency of inter-broker
   * replica movements even if the concurrency adjuster is enabled, {@code false} otherwise.
   */
  public synchronized void executeProposals(Collection<ExecutionProposal> proposals,
                                            Set<Integer> unthrottledBrokers,
                                            Set<Integer> removedBrokers,
                                            LoadMonitor loadMonitor,
                                            Integer requestedInterBrokerPartitionMovementConcurrency,
                                            Integer requestedIntraBrokerPartitionMovementConcurrency,
                                            Integer requestedLeadershipMovementConcurrency,
                                            Long requestedExecutionProgressCheckIntervalMs,
                                            ReplicaMovementStrategy replicaMovementStrategy,
                                            Long replicationThrottle,
                                            boolean isTriggeredByUserRequest,
                                            String uuid,
                                            boolean isKafkaAssignerMode,
                                            boolean skipInterBrokerReplicaConcurrencyAdjustment) throws OngoingExecutionException {
    setExecutionMode(isKafkaAssignerMode);
    sanityCheckExecuteProposals(loadMonitor, uuid);
    _skipInterBrokerReplicaConcurrencyAdjustment = skipInterBrokerReplicaConcurrencyAdjustment;
    try {
      initProposalExecution(proposals, unthrottledBrokers, requestedInterBrokerPartitionMovementConcurrency,
                            requestedIntraBrokerPartitionMovementConcurrency, requestedLeadershipMovementConcurrency,
                            requestedExecutionProgressCheckIntervalMs, replicaMovementStrategy, isTriggeredByUserRequest, loadMonitor);
      startExecution(loadMonitor, null, removedBrokers, replicationThrottle, isTriggeredByUserRequest);
    } catch (Exception e) {
      processExecuteProposalsFailure();
      throw e;
    }
  }

  private void sanityCheckExecuteProposals(LoadMonitor loadMonitor, String uuid) throws OngoingExecutionException {
    if (_hasOngoingExecution) {
      throw new OngoingExecutionException("Cannot execute new proposals while there is an ongoing execution.");
    }
    if (loadMonitor == null) {
      throw new IllegalArgumentException("Load monitor cannot be null.");
    }
    if (_executorState.state() != GENERATING_PROPOSALS_FOR_EXECUTION) {
      throw new IllegalStateException(String.format("Unexpected executor state %s. Initializing proposal execution requires"
                                                    + " generating proposals for execution.", _executorState.state()));
    }
    if (uuid == null || !uuid.equals(_uuid)) {
      throw new IllegalStateException(String.format("Attempt to initialize proposal execution with a UUID %s that differs from"
                                                    + " the UUID used for generating proposals for execution %s.", uuid, _uuid));
    }
  }

  private synchronized void initProposalExecution(Collection<ExecutionProposal> proposals,
                                                  Collection<Integer> brokersToSkipConcurrencyCheck,
                                                  Integer requestedInterBrokerPartitionMovementConcurrency,
                                                  Integer requestedIntraBrokerPartitionMovementConcurrency,
                                                  Integer requestedLeadershipMovementConcurrency,
                                                  Long requestedExecutionProgressCheckIntervalMs,
                                                  ReplicaMovementStrategy replicaMovementStrategy,
                                                  boolean isTriggeredByUserRequest,
                                                  LoadMonitor loadMonitor) {
    _executorState = ExecutorState.initializeProposalExecution(_uuid, _reasonSupplier.get(), recentlyDemotedBrokers(),
                                                               recentlyRemovedBrokers(), isTriggeredByUserRequest);
    _executionTaskManager.setExecutionModeForTaskTracker(_isKafkaAssignerMode);
    _executionTaskManager.addExecutionProposals(proposals, brokersToSkipConcurrencyCheck, _metadataClient.refreshMetadata().cluster(),
                                                replicaMovementStrategy);
    _concurrencyAdjuster.initAdjustment(loadMonitor, requestedInterBrokerPartitionMovementConcurrency, requestedLeadershipMovementConcurrency);
    setRequestedIntraBrokerPartitionMovementConcurrency(requestedIntraBrokerPartitionMovementConcurrency);
    setRequestedExecutionProgressCheckIntervalMs(requestedExecutionProgressCheckIntervalMs);
  }

  /**
   * Initialize proposal execution and start demotion.
   *
   * @param proposals Proposals to be executed.
   * @param demotedBrokers Demoted brokers.
   * @param loadMonitor Load monitor.
   * @param concurrentSwaps The number of concurrent swap operations per broker.
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements
   *                                               (if null, use num.concurrent.leader.movements).
   * @param requestedExecutionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                                  execution (if null, use execution.progress.check.interval.ms).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            while executing demotion proposals (if null, no throttling is applied).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   */
  public synchronized void executeDemoteProposals(Collection<ExecutionProposal> proposals,
                                                  Collection<Integer> demotedBrokers,
                                                  LoadMonitor loadMonitor,
                                                  Integer concurrentSwaps,
                                                  Integer requestedLeadershipMovementConcurrency,
                                                  Long requestedExecutionProgressCheckIntervalMs,
                                                  ReplicaMovementStrategy replicaMovementStrategy,
                                                  Long replicationThrottle,
                                                  boolean isTriggeredByUserRequest,
                                                  String uuid) throws OngoingExecutionException {
    setExecutionMode(false);
    sanityCheckExecuteProposals(loadMonitor, uuid);
    _skipInterBrokerReplicaConcurrencyAdjustment = true;
    try {
      initProposalExecution(proposals, demotedBrokers, concurrentSwaps, 0, requestedLeadershipMovementConcurrency,
                            requestedExecutionProgressCheckIntervalMs, replicaMovementStrategy, isTriggeredByUserRequest, loadMonitor);
      startExecution(loadMonitor, demotedBrokers, null, replicationThrottle, isTriggeredByUserRequest);
    } catch (Exception e) {
      processExecuteProposalsFailure();
      throw e;
    }
  }

  /**
   * Dynamically set the inter-broker partition movement concurrency per broker.
   *
   * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
   *                                                         per broker.
   */
  public void setRequestedInterBrokerPartitionMovementConcurrency(Integer requestedInterBrokerPartitionMovementConcurrency) {
    _executionTaskManager.setRequestedInterBrokerPartitionMovementConcurrency(requestedInterBrokerPartitionMovementConcurrency);
  }

  /**
   * Dynamically set the intra-broker partition movement concurrency.
   *
   * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements.
   */
  public void setRequestedIntraBrokerPartitionMovementConcurrency(Integer requestedIntraBrokerPartitionMovementConcurrency) {
    _executionTaskManager.setRequestedIntraBrokerPartitionMovementConcurrency(requestedIntraBrokerPartitionMovementConcurrency);
  }

  /**
   * Dynamically set the leadership movement concurrency.
   *
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements.
   */
  public void setRequestedLeadershipMovementConcurrency(Integer requestedLeadershipMovementConcurrency) {
    _executionTaskManager.setRequestedLeadershipMovementConcurrency(requestedLeadershipMovementConcurrency);
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   */
  private void setExecutionMode(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Allow a reference to {@link UserTaskManager} to be set externally and allow executor to retrieve information
   * about the request that generated execution.
   * @param userTaskManager a reference to {@link UserTaskManager} instance.
   */
  public void setUserTaskManager(UserTaskManager userTaskManager) {
    _userTaskManager = userTaskManager;
  }

  private int numExecutionStopped() {
    return _numExecutionStopped.get();
  }

  private int numExecutionStoppedByUser() {
    return _numExecutionStoppedByUser.get();
  }

  private int numExecutionStartedInKafkaAssignerMode() {
    return _numExecutionStartedInKafkaAssignerMode.get();
  }

  private int numExecutionStartedInNonKafkaAssignerMode() {
    return _numExecutionStartedInNonKafkaAssignerMode.get();
  }

  /**
   * Pause the load monitor and kick off the execution.
   *
   * @param loadMonitor Load monitor.
   * @param demotedBrokers Brokers to be demoted, null if no broker has been demoted.
   * @param removedBrokers Brokers to be removed, null if no broker has been removed.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            while moving partitions (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   */
  private void startExecution(LoadMonitor loadMonitor,
                              Collection<Integer> demotedBrokers,
                              Collection<Integer> removedBrokers,
                              Long replicationThrottle,
                              boolean isTriggeredByUserRequest) throws OngoingExecutionException {
    _executionStoppedByUser.set(false);
    sanityCheckOngoingMovement();
    _hasOngoingExecution = true;
    _anomalyDetectorManager.maybeClearOngoingAnomalyDetectionTimeMs();
    _anomalyDetectorManager.resetHasUnfixableGoals();
    _stopSignal.set(NO_STOP_EXECUTION);
    _executionStoppedByUser.set(false);
    if (_isKafkaAssignerMode) {
      _numExecutionStartedInKafkaAssignerMode.incrementAndGet();
    } else {
      _numExecutionStartedInNonKafkaAssignerMode.incrementAndGet();
    }
    _proposalExecutor.submit(
        new ProposalExecutionRunnable(loadMonitor, demotedBrokers, removedBrokers, replicationThrottle, isTriggeredByUserRequest));
  }

  /**
   * Sanity check whether there are ongoing (1) inter-broker or intra-broker replica movements or (2) leadership movements.
   * This check ensures lack of ongoing movements started by external agents -- i.e. not started by this Executor.
   */
  private void sanityCheckOngoingMovement() throws OngoingExecutionException {
    if (hasOngoingPartitionReassignments()) {
      throw new OngoingExecutionException("There are ongoing inter-broker partition movements.");
    } else {
      boolean hasOngoingIntraBrokerReplicaMovement;
      try {
        hasOngoingIntraBrokerReplicaMovement =
            hasOngoingIntraBrokerReplicaMovement(_metadataClient.cluster().nodes().stream().mapToInt(Node::id).boxed()
                                                                .collect(Collectors.toSet()), _adminClient, _config);
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
        // This may indicate transient (e.g. network) issues.
        throw new IllegalStateException("Failed to retrieve if there are already ongoing intra-broker replica reassignments.", e);
      }
      if (hasOngoingIntraBrokerReplicaMovement) {
        throw new OngoingExecutionException("There are ongoing intra-broker partition movements.");
      } else if (hasOngoingLeaderElection()) {
        throw new OngoingExecutionException("There are ongoing leadership movements.");
      }
    }
  }

  private void processExecuteProposalsFailure() {
    _executionTaskManager.clear();
    _uuid = null;
    _reasonSupplier = null;
    _executorState = ExecutorState.noTaskInProgress(recentlyDemotedBrokers(), recentlyRemovedBrokers());
  }

  /**
   * Notify the executor on starting to generate proposals for execution with the given uuid and reason supplier.
   * The executor must be in {@link ExecutorState.State#NO_TASK_IN_PROGRESS} state for this operation to succeed.
   *
   * @param uuid UUID of the current execution.
   * @param reasonSupplier Reason supplier for the execution.
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   */
  public synchronized void setGeneratingProposalsForExecution(String uuid, Supplier<String> reasonSupplier, boolean isTriggeredByUserRequest)
      throws OngoingExecutionException {
    ExecutorState.State currentExecutorState = _executorState.state();
    if (currentExecutorState != NO_TASK_IN_PROGRESS) {
      throw new OngoingExecutionException(String.format("Cannot generate proposals while the executor is in %s state.", currentExecutorState));
    }

    _uuid = validateNotNull(uuid, "UUID of the execution cannot be null.");
    _reasonSupplier = validateNotNull(reasonSupplier, "Reason supplier cannot be null.");
    _executorState = ExecutorState.generatingProposalsForExecution(_uuid, _reasonSupplier.get(), recentlyDemotedBrokers(),
                                                                   recentlyRemovedBrokers(), isTriggeredByUserRequest);
  }

  /**
   * Notify the executor on the failure to generate proposals for execution with the given uuid.
   * The executor may stuck in {@link ExecutorState.State#GENERATING_PROPOSALS_FOR_EXECUTION} state if this call is omitted.
   *
   * This is a no-op if called when
   * <ul>
   *   <li>Not generating proposals for execution: This is because all other execution failures after generating proposals
   *   for execution are already handled by the executor.</li>
   *   <li>Generating proposals with a different UUID: This indicates an error in the caller-side logic, and is not
   *   supposed to happen because at any given time proposals shall be generated for only one request -- for detecting
   *   such presumably benign misuse (if any), this case is logged as a warning.</li>
   * </ul>
   * @param uuid UUID of the failed proposal generation for execution.
   */
  public synchronized void failGeneratingProposalsForExecution(String uuid) {
    if (_executorState.state() == GENERATING_PROPOSALS_FOR_EXECUTION) {
      if (uuid != null && uuid.equals(_uuid)) {
        LOG.info("Failed to generate proposals for execution (UUID: {} reason: {}).", uuid, _reasonSupplier.get());
        _uuid = null;
        _reasonSupplier = null;
        _executorState = ExecutorState.noTaskInProgress(recentlyDemotedBrokers(), recentlyRemovedBrokers());
      } else {
        LOG.warn("UUID mismatch in attempt to report failure to generate proposals (received: {} expected: {})", uuid, _uuid);
      }
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   *
   * @param forceExecutionStop Whether force execution to stop.
   */
  public synchronized void userTriggeredStopExecution(boolean forceExecutionStop) {
    if (stopExecution(forceExecutionStop)) {
      LOG.info("User requested to stop the ongoing proposal execution" + (forceExecutionStop ? " forcefully." : "."));
      _numExecutionStoppedByUser.incrementAndGet();
      _executionStoppedByUser.set(true);
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   *
   * @param forceExecutionStop Whether force execution to stop.
   * @return True if the flag to stop the execution is set after the call (i.e. was not set already), false otherwise.
   */
  private synchronized boolean stopExecution(boolean forceExecutionStop) {
    if ((forceExecutionStop && (_stopSignal.compareAndSet(NO_STOP_EXECUTION, FORCE_STOP_EXECUTION) ||
                                _stopSignal.compareAndSet(STOP_EXECUTION, FORCE_STOP_EXECUTION))) ||
        (!forceExecutionStop && _stopSignal.compareAndSet(NO_STOP_EXECUTION, STOP_EXECUTION)))  {
      _numExecutionStopped.incrementAndGet();
      _executionTaskManager.setStopRequested();
      return true;
    }
    return false;
  }

  /**
   * Shutdown the executor.
   */
  public synchronized void shutdown() {
    LOG.info("Shutting down executor.");
    if (_hasOngoingExecution) {
      LOG.warn("Shutdown executor may take long because execution is still in progress.");
    }
    _proposalExecutor.shutdown();

    try {
      _proposalExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for anomaly detector to shutdown.");
    }
    _metadataClient.close();
    KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(_kafkaZkClient);
    KafkaCruiseControlUtils.closeAdminClientWithTimeout(_adminClient);
    _executionHistoryScannerExecutor.shutdownNow();
    _concurrencyAdjusterExecutor.shutdownNow();
    LOG.info("Executor shutdown completed.");
  }

  /**
   * Let executor know the intention regarding modifying the ongoing execution. Only one request at a given time is
   * allowed to modify the ongoing execution.
   *
   * @param modify True to indicate, false to cancel the intention to modify
   * @return True if the intention changes the state known by executor, false otherwise.
   */
  public boolean modifyOngoingExecution(boolean modify) {
    return _ongoingExecutionIsBeingModified.compareAndSet(!modify, modify);
  }

  /**
   * Whether there is an ongoing operation triggered by current Cruise Control deployment.
   *
   * @return True if there is an ongoing execution.
   */
  public boolean hasOngoingExecution() {
    return _hasOngoingExecution;
  }

  /**
   * Check whether there is an ongoing partition reassignment on Kafka cluster.
   *
   * Note that a {@code false} response does not guarantee lack of an ongoing execution because when there is an ongoing
   * execution inside Cruise Control, partition reassignment task batches are sent to Kafka periodically. So, there will
   * be intervals without partition reassignments.
   *
   * @return True if there is an ongoing partition reassignment on Kafka cluster.
   */
  public boolean hasOngoingPartitionReassignments() {
    return !ExecutorUtils.partitionsBeingReassigned(_kafkaZkClient).isEmpty();
  }

  /**
   * Check whether there is an ongoing leadership reassignment.
   * This method directly checks the existence of zNode in /admin/preferred_replica_election.
   *
   * @return True if there is an ongoing leadership reassignment.
   */
  public boolean hasOngoingLeaderElection() {
    return !ExecutorUtils.ongoingLeaderElection(_kafkaZkClient).isEmpty();
  }

  /**
   * This class is thread safe.
   *
   * Note that once the thread for {@link ProposalExecutionRunnable} is submitted for running, the variable
   * _executionTaskManager can only be written within this inner class, but not from the outer Executor class.
   */
  private class ProposalExecutionRunnable implements Runnable {
    private final LoadMonitor _loadMonitor;
    private final Set<Integer> _recentlyDemotedBrokers;
    private final Set<Integer> _recentlyRemovedBrokers;
    private final Long _replicationThrottle;
    private Throwable _executionException;
    private final boolean _isTriggeredByUserRequest;
    private long _lastSlowTaskReportingTimeMs;
    private static final boolean FORCE_PAUSE_SAMPLING = true;

    ProposalExecutionRunnable(LoadMonitor loadMonitor,
                              Collection<Integer> demotedBrokers,
                              Collection<Integer> removedBrokers,
                              Long replicationThrottle,
                              boolean isTriggeredByUserRequest) {
      _loadMonitor = loadMonitor;
      _executionException = null;
      if (isTriggeredByUserRequest && _userTaskManager == null) {
        processExecuteProposalsFailure();
        _hasOngoingExecution = false;
        _stopSignal.set(NO_STOP_EXECUTION);
        _executionStoppedByUser.set(false);
        LOG.error("Failed to initialize proposal execution.");
        throw new IllegalStateException("User task manager cannot be null.");
      }
      if (demotedBrokers != null) {
        // Add/overwrite the latest demotion time of (non-permanent) demoted brokers (if any).
        demotedBrokers.forEach(id -> {
          Long demoteStartTime = _latestDemoteStartTimeMsByBrokerId.get(id);
          if (demoteStartTime == null || demoteStartTime != ExecutionUtils.PERMANENT_TIMESTAMP) {
            _latestDemoteStartTimeMsByBrokerId.put(id, _time.milliseconds());
          }
        });
      }
      if (removedBrokers != null) {
        // Add/overwrite the latest removal time of (non-permanent) removed brokers (if any).
        removedBrokers.forEach(id -> {
          Long removeStartTime = _latestRemoveStartTimeMsByBrokerId.get(id);
          if (removeStartTime == null || removeStartTime != ExecutionUtils.PERMANENT_TIMESTAMP) {
            _latestRemoveStartTimeMsByBrokerId.put(id, _time.milliseconds());
          }
        });
      }
      _recentlyDemotedBrokers = recentlyDemotedBrokers();
      _recentlyRemovedBrokers = recentlyRemovedBrokers();
      _replicationThrottle = replicationThrottle;
      _isTriggeredByUserRequest = isTriggeredByUserRequest;
      _lastSlowTaskReportingTimeMs = -1L;
    }

    public void run() {
      LOG.info("Starting executing balancing proposals.");
      UserTaskManager.UserTaskInfo userTaskInfo = initExecution();
      execute(userTaskInfo);
      LOG.info("Execution finished.");
    }

    private UserTaskManager.UserTaskInfo initExecution() {
      UserTaskManager.UserTaskInfo userTaskInfo = null;
      // If the task is triggered from a user request, mark the task to be in-execution state in user task manager and
      // retrieve the associated user task information.
      if (_isTriggeredByUserRequest) {
        userTaskInfo = _userTaskManager.markTaskExecutionBegan(_uuid);
      }
      String reason = _reasonSupplier.get();
      _executorState = ExecutorState.executionStarting(_uuid, reason, _recentlyDemotedBrokers, _recentlyRemovedBrokers, _isTriggeredByUserRequest);
      OPERATION_LOG.info("Task [{}] execution starts. The reason of execution is {}.", _uuid, reason);
      _ongoingExecutionIsBeingModified.set(false);

      return userTaskInfo;
    }

    /**
     * Unless the metric sampling is already in the desired mode:
     * <ul>
     *   <li>Pause the metric sampling</li>
     *   <li>Set the metric sampling mode to retrieve desired samples</li>
     *   <li>Resume the metric sampling</li>
     * </ul>
     */
    private void adjustSamplingModeBeforeExecution() throws InterruptedException {
      // Pause the partition metric sampling to avoid the loss of accuracy during execution.
      while (_loadMonitor.samplingMode() != BROKER_METRICS_ONLY) {
        try {
          String reasonForPause = String.format("Paused-By-Cruise-Control-Before-Starting-Execution (Date: %s)", currentUtcDate());
          _loadMonitor.pauseMetricSampling(reasonForPause, FORCE_PAUSE_SAMPLING);
          // Set the metric sampling mode to retrieve broker samples only.
          _loadMonitor.setSamplingMode(BROKER_METRICS_ONLY);
          break;
        } catch (IllegalStateException e) {
          Thread.sleep(executionProgressCheckIntervalMs());
          LOG.debug("Waiting for the load monitor to be ready to adjust sampling mode.", e);
        }
      }
      // Resume the metric sampling.
      _loadMonitor.resumeMetricSampling(String.format("Resumed-By-Cruise-Control-Before-Starting-Execution (Date: %s)", currentUtcDate()));
    }

    /**
     * Start the actual execution of the proposals in order:
     * <ol>
     *   <li>Inter-broker move replicas.</li>
     *   <li>Intra-broker move replicas.</li>
     *   <li>Transfer leadership.</li>
     * </ol>
     *
     * @param userTaskInfo The task information if the task is triggered from a user request, {@code null} otherwise.
     */
    private void execute(UserTaskManager.UserTaskInfo userTaskInfo) {
      try {
        // Ensure that sampling mode is adjusted properly to avoid the loss of partition metric accuracy
        // and enable the collection of broker metric samples during an ongoing execution.
        adjustSamplingModeBeforeExecution();

        // 1. Inter-broker move replicas if possible.
        if (_executorState.state() == STARTING_EXECUTION) {
          _executorState = ExecutorState.operationInProgress(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(INTER_BROKER_REPLICA_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _reasonSupplier.get(),
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers,
                                                             _isTriggeredByUserRequest);
          interBrokerMoveReplicas();
          updateOngoingExecutionState();
        }

        // 2. Intra-broker move replicas if possible.
        if (_executorState.state() == INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          _executorState = ExecutorState.operationInProgress(INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _reasonSupplier.get(),
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers,
                                                             _isTriggeredByUserRequest);
          intraBrokerMoveReplicas();
          updateOngoingExecutionState();
        }

        // 3. Transfer leadership if possible.
        if (_executorState.state() == INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          _executorState = ExecutorState.operationInProgress(LEADER_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(LEADER_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _reasonSupplier.get(),
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers,
                                                             _isTriggeredByUserRequest);
          moveLeaderships();
          updateOngoingExecutionState();
        }

        // 4. Delete tasks from Zookeeper if needed.
        if (_executorState.state() == STOPPING_EXECUTION && _stopSignal.get() == FORCE_STOP_EXECUTION) {
          ExecutionUtils.deleteZNodesToStopExecution(_kafkaZkClient);
        }
      } catch (Throwable t) {
        LOG.error("Executor got exception during execution", t);
        _executionException = t;
      } finally {
        notifyFinishedTask(userTaskInfo);
        // Clear completed execution.
        clearCompletedExecution();
      }
    }

    private void notifyFinishedTask(UserTaskManager.UserTaskInfo userTaskInfo) {
      // If the finished task was triggered by a user request, update task status in user task manager; if task is triggered
      // by an anomaly self-healing, update the task status in anomaly detector.
      if (userTaskInfo != null) {
        _userTaskManager.markTaskExecutionFinished(_uuid, _executorState.state() == STOPPING_EXECUTION || _executionException != null);
      } else {
        _anomalyDetectorManager.markSelfHealingFinished(_uuid);
      }

      String prefix = String.format("Task [%s] %s execution is ", _uuid,
                                    userTaskInfo != null ? ("user" + userTaskInfo.requestUrl()) : "self-healing");

      if (_executorState.state() == STOPPING_EXECUTION) {
        notifyExecutionFinished(String.format("%sstopped by %s.", prefix, _executionStoppedByUser.get() ? "user" : "Cruise Control"),
                                true);
      } else if (_executionException != null) {
        notifyExecutionFinished(String.format("%sinterrupted with exception %s.", prefix, _executionException.getMessage()),
                                true);
      } else {
        notifyExecutionFinished(String.format("%sfinished.", prefix), false);
      }
    }

    private void notifyExecutionFinished(String message, boolean isWarning) {
      if (isWarning) {
        _executorNotifier.sendAlert(message);
        OPERATION_LOG.warn(message);
      } else {
        _executorNotifier.sendNotification(message);
        OPERATION_LOG.info(message);
      }
    }

    private void clearCompletedExecution() {
      _executionTaskManager.clear();
      _uuid = null;
      _reasonSupplier = null;
      _executorState = ExecutorState.noTaskInProgress(_recentlyDemotedBrokers, _recentlyRemovedBrokers);
      _hasOngoingExecution = false;
      _stopSignal.set(NO_STOP_EXECUTION);
      _executionStoppedByUser.set(false);
      // Ensure that sampling mode is adjusted properly to continue collecting partition metrics after execution.
      _loadMonitor.setSamplingMode(ALL);
    }

    private void updateOngoingExecutionState() {
      if (_stopSignal.get() == NO_STOP_EXECUTION) {
        switch (_executorState.state()) {
          case LEADER_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(LEADER_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(LEADER_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _reasonSupplier.get(),
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers,
                                                               _isTriggeredByUserRequest);
            break;
          case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(INTER_BROKER_REPLICA_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _reasonSupplier.get(),
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers,
                                                               _isTriggeredByUserRequest);
            break;
          case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _reasonSupplier.get(),
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers,
                                                               _isTriggeredByUserRequest);
            break;
          default:
            throw new IllegalStateException("Unexpected ongoing execution state " + _executorState.state());
        }
      } else {
        Set<ExecutionTask.TaskType> taskTypesToGetFullList = new HashSet<>(ExecutionTask.TaskType.cachedValues());
        _executorState = ExecutorState.operationInProgress(STOPPING_EXECUTION,
                                                           _executionTaskManager.getExecutionTasksSummary(taskTypesToGetFullList),
                                                           _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                           _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                           _executionTaskManager.leadershipMovementConcurrency(),
                                                           _uuid,
                                                           _reasonSupplier.get(),
                                                           _recentlyDemotedBrokers,
                                                           _recentlyRemovedBrokers,
                                                           _isTriggeredByUserRequest);
      }
    }

    private void interBrokerMoveReplicas() {
      ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_kafkaZkClient, _replicationThrottle);
      int numTotalPartitionMovements = _executionTaskManager.numRemainingInterBrokerPartitionMovements();
      long totalDataToMoveInMB = _executionTaskManager.remainingInterBrokerDataToMoveInMB();
      LOG.info("Starting {} inter-broker partition movements.", numTotalPartitionMovements);

      int partitionsToMove = numTotalPartitionMovements;
      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || !inExecutionTasks().isEmpty()) && _stopSignal.get() == NO_STOP_EXECUTION) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getInterBrokerReplicaMovementTasks();
        LOG.info("Executor will execute {} task(s)", tasksToExecute.size());

        if (!tasksToExecute.isEmpty()) {
          throttleHelper.setThrottles(tasksToExecute.stream().map(ExecutionTask::proposal).collect(Collectors.toList()));
          // Execute the tasks.
          _executionTaskManager.markTasksInProgress(tasksToExecute);
          ExecutorUtils.executeReplicaReassignmentTasks(_kafkaZkClient, tasksToExecute);
        }
        // Wait indefinitely for partition movements to finish.
        List<ExecutionTask> completedTasks = waitForExecutionTaskToFinish();
        partitionsToMove = _executionTaskManager.numRemainingInterBrokerPartitionMovements();
        int numFinishedPartitionMovements = _executionTaskManager.numFinishedInterBrokerPartitionMovements();
        long finishedDataMovementInMB = _executionTaskManager.finishedInterBrokerDataMovementInMB();
        LOG.info("{}/{} ({}%) inter-broker partition movements completed. {}/{} ({}%) MB have been moved.",
                 numFinishedPartitionMovements, numTotalPartitionMovements,
                 String.format("%.2f", numFinishedPartitionMovements * UNIT_INTERVAL_TO_PERCENTAGE / numTotalPartitionMovements),
                 finishedDataMovementInMB, totalDataToMoveInMB,
                 totalDataToMoveInMB == 0 ? 100 : String.format("%.2f", finishedDataMovementInMB * UNIT_INTERVAL_TO_PERCENTAGE
                                                                        / totalDataToMoveInMB));
        throttleHelper.clearThrottles(completedTasks,
                tasksToExecute.stream().filter(t -> t.state() == ExecutionTaskState.IN_PROGRESS).collect(Collectors.toList()));
      }
      // After the partition movement finishes, wait for the controller to clean the reassignment zkPath. This also
      // ensures a clean stop when the execution is stopped in the middle.
      Set<ExecutionTask> inExecutionTasks = inExecutionTasks();
      while (!inExecutionTasks.isEmpty()) {
        LOG.info("Waiting for {} tasks moving {} MB to finish.", inExecutionTasks.size(),
                 _executionTaskManager.inExecutionInterBrokerDataToMoveInMB());
        List<ExecutionTask> completedRemainingTasks = waitForExecutionTaskToFinish();
        inExecutionTasks = inExecutionTasks();
        throttleHelper.clearThrottles(completedRemainingTasks, new ArrayList<>(inExecutionTasks));
      }
      if (inExecutionTasks().isEmpty()) {
        LOG.info("Inter-broker partition movements finished.");
      } else if (_stopSignal.get() != NO_STOP_EXECUTION) {
        ExecutionTasksSummary executionTasksSummary = _executionTaskManager.getExecutionTasksSummary(Collections.emptySet());
        Map<ExecutionTaskState, Integer> partitionMovementTasksByState = executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
        LOG.info("Inter-broker partition movements stopped. For inter-broker partition movements {} tasks cancelled, {} tasks in-progress, "
                 + "{} tasks aborting, {} tasks aborted, {} tasks dead, {} tasks completed, {} remaining data to move; for intra-broker "
                 + "partition movement {} tasks cancelled; for leadership movements {} task cancelled.",
                 partitionMovementTasksByState.get(ExecutionTaskState.PENDING),
                 partitionMovementTasksByState.get(ExecutionTaskState.IN_PROGRESS),
                 partitionMovementTasksByState.get(ExecutionTaskState.ABORTING),
                 partitionMovementTasksByState.get(ExecutionTaskState.ABORTED),
                 partitionMovementTasksByState.get(ExecutionTaskState.DEAD),
                 partitionMovementTasksByState.get(ExecutionTaskState.COMPLETED),
                 executionTasksSummary.remainingInterBrokerDataToMoveInMB(),
                 executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.PENDING),
                 executionTasksSummary.taskStat().get(LEADER_ACTION).get(ExecutionTaskState.PENDING));
      }
    }

    private void intraBrokerMoveReplicas() {
      int numTotalPartitionMovements = _executionTaskManager.numRemainingIntraBrokerPartitionMovements();
      long totalDataToMoveInMB = _executionTaskManager.remainingIntraBrokerDataToMoveInMB();
      LOG.info("Starting {} intra-broker partition movements.", numTotalPartitionMovements);

      int partitionsToMove = numTotalPartitionMovements;
      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || !inExecutionTasks().isEmpty()) && _stopSignal.get() == NO_STOP_EXECUTION) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getIntraBrokerReplicaMovementTasks();
        LOG.info("Executor will execute {} task(s)", tasksToExecute.size());

        if (!tasksToExecute.isEmpty()) {
          // Execute the tasks.
          _executionTaskManager.markTasksInProgress(tasksToExecute);
          executeIntraBrokerReplicaMovements(tasksToExecute, _adminClient, _executionTaskManager, _config);
        }
        // Wait indefinitely for partition movements to finish.
        waitForExecutionTaskToFinish();
        partitionsToMove = _executionTaskManager.numRemainingIntraBrokerPartitionMovements();
        int numFinishedPartitionMovements = _executionTaskManager.numFinishedIntraBrokerPartitionMovements();
        long finishedDataToMoveInMB = _executionTaskManager.finishedIntraBrokerDataToMoveInMB();
        LOG.info("{}/{} ({}%) intra-broker partition movements completed. {}/{} ({}%) MB have been moved.",
                 numFinishedPartitionMovements, numTotalPartitionMovements,
                 String.format("%.2f", numFinishedPartitionMovements * UNIT_INTERVAL_TO_PERCENTAGE / numTotalPartitionMovements),
                 finishedDataToMoveInMB, totalDataToMoveInMB,
                 totalDataToMoveInMB == 0 ? 100 : String.format("%.2f", finishedDataToMoveInMB * UNIT_INTERVAL_TO_PERCENTAGE
                                                                        / totalDataToMoveInMB));
      }
      Set<ExecutionTask> inExecutionTasks = inExecutionTasks();
      while (!inExecutionTasks.isEmpty()) {
        LOG.info("Waiting for {} tasks moving {} MB to finish", inExecutionTasks.size(),
                 _executionTaskManager.inExecutionIntraBrokerDataMovementInMB());
        waitForExecutionTaskToFinish();
        inExecutionTasks = inExecutionTasks();
      }
      if (inExecutionTasks().isEmpty()) {
        LOG.info("Intra-broker partition movements finished.");
      } else if (_stopSignal.get() != NO_STOP_EXECUTION) {
        ExecutionTasksSummary executionTasksSummary = _executionTaskManager.getExecutionTasksSummary(Collections.emptySet());
        Map<ExecutionTaskState, Integer> partitionMovementTasksByState = executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
        LOG.info("Intra-broker partition movements stopped. For intra-broker partition movements {} tasks cancelled, {} tasks in-progress, "
                 + "{} tasks aborting, {} tasks aborted, {} tasks dead, {} tasks completed, {} remaining data to move; for leadership "
                 + "movements {} task cancelled.",
                 partitionMovementTasksByState.get(ExecutionTaskState.PENDING),
                 partitionMovementTasksByState.get(ExecutionTaskState.IN_PROGRESS),
                 partitionMovementTasksByState.get(ExecutionTaskState.ABORTING),
                 partitionMovementTasksByState.get(ExecutionTaskState.ABORTED),
                 partitionMovementTasksByState.get(ExecutionTaskState.DEAD),
                 partitionMovementTasksByState.get(ExecutionTaskState.COMPLETED),
                 executionTasksSummary.remainingIntraBrokerDataToMoveInMB(),
                 executionTasksSummary.taskStat().get(LEADER_ACTION).get(ExecutionTaskState.PENDING));
      }
    }

    private void moveLeaderships() {
      int numTotalLeadershipMovements = _executionTaskManager.numRemainingLeadershipMovements();
      LOG.info("Starting {} leadership movements.", numTotalLeadershipMovements);
      int numFinishedLeadershipMovements = 0;
      while (_executionTaskManager.numRemainingLeadershipMovements() != 0 && _stopSignal.get() == NO_STOP_EXECUTION) {
        updateOngoingExecutionState();
        numFinishedLeadershipMovements += moveLeadershipInBatch();
        LOG.info("{}/{} ({}%) leadership movements completed.", numFinishedLeadershipMovements,
                 numTotalLeadershipMovements, numFinishedLeadershipMovements * 100 / numTotalLeadershipMovements);
      }
      if (inExecutionTasks().isEmpty()) {
        LOG.info("Leadership movements finished.");
      } else if (_stopSignal.get() != NO_STOP_EXECUTION) {
        Map<ExecutionTaskState, Integer> leadershipMovementTasksByState =
            _executionTaskManager.getExecutionTasksSummary(Collections.emptySet()).taskStat().get(LEADER_ACTION);
        LOG.info("Leadership movements stopped. {} tasks cancelled, {} tasks in-progress, {} tasks aborting, {} tasks aborted, "
                 + "{} tasks dead, {} tasks completed.",
                 leadershipMovementTasksByState.get(ExecutionTaskState.PENDING),
                 leadershipMovementTasksByState.get(ExecutionTaskState.IN_PROGRESS),
                 leadershipMovementTasksByState.get(ExecutionTaskState.ABORTING),
                 leadershipMovementTasksByState.get(ExecutionTaskState.ABORTED),
                 leadershipMovementTasksByState.get(ExecutionTaskState.DEAD),
                 leadershipMovementTasksByState.get(ExecutionTaskState.COMPLETED));
      }
    }

    private int moveLeadershipInBatch() {
      List<ExecutionTask> leadershipMovementTasks = _executionTaskManager.getLeadershipMovementTasks();
      int numLeadershipToMove = leadershipMovementTasks.size();
      LOG.debug("Executing {} leadership movements in a batch.", numLeadershipToMove);
      // Execute the leadership movements.
      if (!leadershipMovementTasks.isEmpty() && _stopSignal.get() == NO_STOP_EXECUTION) {
        // Run preferred leader election unless there is already an ongoing leadership movement.
        while (hasOngoingLeaderElection()) {
          try {
            LOG.error("Waiting for Kafka Controller to delete /admin/preferred_replica_election zNode. Are other admin "
                      + "tools triggering a PLE?");
            Thread.sleep(executionProgressCheckIntervalMs());
          } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for Kafka Controller to delete /admin/preferred_replica_election zNode.");
          }
        }
        // Mark leadership movements in progress.
        _executionTaskManager.markTasksInProgress(leadershipMovementTasks);

        ExecutorUtils.executePreferredLeaderElection(_kafkaZkClient, leadershipMovementTasks);
        LOG.trace("Waiting for leadership movement batch to finish.");
        while (!inExecutionTasks().isEmpty() && _stopSignal.get() == NO_STOP_EXECUTION) {
          waitForExecutionTaskToFinish();
        }
      }
      return numLeadershipToMove;
    }

    /**
     * Periodically checks the metadata to see if (1) partition or (2) leadership reassignment has finished or not.
     * @return Finished tasks.
     */
    private List<ExecutionTask> waitForExecutionTaskToFinish() {
      List<ExecutionTask> finishedTasks = new ArrayList<>();
      Set<Long> forceStoppedTaskIds = new HashSet<>();
      Set<Long> deletedTaskIds = new HashSet<>();
      Set<Long> deadOrAbortingTaskIds = new HashSet<>();
      do {
        // If there is no finished tasks, we need to check if anything is blocked.
        maybeReexecuteTasks();
        try {
          Thread.sleep(executionProgressCheckIntervalMs());
        } catch (InterruptedException e) {
          // let it go
        }

        Cluster cluster = _metadataClient.refreshMetadata().cluster();
        Map<ExecutionTask, ReplicaLogDirInfo> logDirInfoByTask = getLogdirInfoForExecutionTask(
            _executionTaskManager.inExecutionTasks(Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
            _adminClient, _config);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Tasks in execution: {}", inExecutionTasks());
        }
        List<ExecutionTask> deadOrAbortingInterBrokerReplicaActions = new ArrayList<>();
        List<ExecutionTask> slowTasksToReport = new ArrayList<>();
        boolean shouldReportSlowTasks = _time.milliseconds() - _lastSlowTaskReportingTimeMs > _slowTaskAlertingBackoffTimeMs;
        for (ExecutionTask task : inExecutionTasks()) {
          TopicPartition tp = task.proposal().topicPartition();
          if (_stopSignal.get() == FORCE_STOP_EXECUTION) {
            LOG.debug("Task {} is marked as dead to force execution to stop.", task);
            finishedTasks.add(task);
            forceStoppedTaskIds.add(task.executionId());
            _executionTaskManager.markTaskDead(task);
          } else if (cluster.partition(tp) == null) {
            // Handle topic deletion during the execution.
            LOG.debug("Task {} is marked as finished because the topic has been deleted", task);
            finishedTasks.add(task);
            deletedTaskIds.add(task.executionId());
            _executionTaskManager.markTaskAborting(task);
            _executionTaskManager.markTaskDone(task);
          } else if (ExecutionUtils.isTaskDone(cluster, logDirInfoByTask, task)) {
            // Check to see if the task is done.
            finishedTasks.add(task);
            _executionTaskManager.markTaskDone(task);
          } else {
            if (shouldReportSlowTasks) {
              task.maybeReportExecutionTooSlow(_time.milliseconds(), slowTasksToReport);
            }
            if (maybeMarkTaskAsDeadOrAborting(cluster, logDirInfoByTask, task)) {
              deadOrAbortingTaskIds.add(task.executionId());
              // Only add the dead or aborted tasks to execute if it is an inter-broker replica action.
              if (task.type() == INTER_BROKER_REPLICA_ACTION) {
                deadOrAbortingInterBrokerReplicaActions.add(task);
              }
              // A dead or aborted task is considered as finished.
              if (task.state() == ExecutionTaskState.DEAD || task.state() == ExecutionTaskState.ABORTED) {
                finishedTasks.add(task);
              }
            }
          }
        }
        sendSlowExecutionAlert(slowTasksToReport);
        handleDeadOrAbortingReplicaActions(deadOrAbortingInterBrokerReplicaActions);
        updateOngoingExecutionState();
      } while (!inExecutionTasks().isEmpty() && finishedTasks.isEmpty());

      LOG.info("Finished tasks: {}.{}{}{}", finishedTasks,
               forceStoppedTaskIds.isEmpty() ? "" : String.format(". [Force-stopped: %s]", forceStoppedTaskIds),
               deletedTaskIds.isEmpty() ? "" : String.format(". [Deleted: %s]", deletedTaskIds),
               deadOrAbortingTaskIds.isEmpty() ? "" : String.format(". [Dead/aborting: %s]", deadOrAbortingTaskIds));

      return finishedTasks;
    }

    private void handleDeadOrAbortingReplicaActions(List<ExecutionTask> deadOrAbortingInterBrokerReplicaActions) {
      if (!deadOrAbortingInterBrokerReplicaActions.isEmpty()) {
        // TODO: Rollback tasks when KIP-455 is available.
        // ExecutorAdminUtils.executeReplicaReassignmentTasks(_kafkaZkClient, deadOrAbortingTasks);
        if (_stopSignal.get() == NO_STOP_EXECUTION) {
          // If there is task aborted or dead, we stop the execution.
          LOG.info("Stop the ongoing execution due to {} dead or aborting tasks: {}.",
                   deadOrAbortingInterBrokerReplicaActions.size(), deadOrAbortingInterBrokerReplicaActions);
          stopExecution(false);
        }
      }
    }

    private void sendSlowExecutionAlert(List<ExecutionTask> slowTasksToReport) {
      if (!slowTasksToReport.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        sb.append("Slow tasks are detected:\n");
        for (ExecutionTask task: slowTasksToReport) {
          sb.append(String.format("\tID: %s\tstart_time:%s\tdetail:%s%n", task.executionId(),
                                  toDateString(task.startTimeMs(), DATE_FORMAT, TIME_ZONE), task));
        }
        _executorNotifier.sendAlert(sb.toString());
        _lastSlowTaskReportingTimeMs = _time.milliseconds();
      }
    }

    /**
     * Mark the task as aborting or dead if needed.
     *
     * Ideally, the task should be marked as:
     * 1. ABORTING: when the execution is stopped by the users.
     * 2. DEAD: when any replica in the new replica list is dead. Or when a leader action times out.
     *
     * Support for stopping ongoing executions with rollback if the execution is stopped by users is
     * pending https://github.com/linkedin/cruise-control/issues/1195.
     *
     * The current actual behavior sets the task state to:
     * 1. IN_PROGRESS: when the execution is stopped by the users. i.e. do nothing but let the task finish normally.
     * 2. DEAD: when the destination broker&disk is dead. i.e. do not block on the execution.
     *
     * @param cluster the kafka cluster
     * @param logdirInfoByTask  disk information for ongoing intra-broker replica movement tasks
     * @param task the task to check
     * @return True if the task is marked as dead or aborting, false otherwise.
     */
    private boolean maybeMarkTaskAsDeadOrAborting(Cluster cluster,
                                                  Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask,
                                                  ExecutionTask task) {
      // Only check tasks with IN_PROGRESS or ABORTING state.
      if (task.state() == ExecutionTaskState.IN_PROGRESS || task.state() == ExecutionTaskState.ABORTING) {
        switch (task.type()) {
          case LEADER_ACTION:
            if (cluster.nodeById(task.proposal().newLeader().brokerId()) == null) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("Killing execution for task {} because the target leader is down.", task);
              return true;
            } else if (_time.milliseconds() > task.startTimeMs() + _leaderMovementTimeoutMs) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("Killing execution for task {} because it took longer than {} to finish.", task, _leaderMovementTimeoutMs);
              return true;
            }
            break;

          case INTER_BROKER_REPLICA_ACTION:
            for (ReplicaPlacementInfo broker : task.proposal().newReplicas()) {
              if (cluster.nodeById(broker.brokerId()) == null) {
                _executionTaskManager.markTaskDead(task);
                LOG.warn("Killing execution for task {} because the new replica {} is down.", task, broker);
                return true;
              }
            }
            break;

          case INTRA_BROKER_REPLICA_ACTION:
            if (!logdirInfoByTask.containsKey(task)) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("Killing execution for task {} because the destination disk is down.", task);
              return true;
            }
            break;

          default:
            throw new IllegalStateException("Unknown task type " + task.type());
        }
      }
      return false;
    }

    private boolean maybeReexecuteInterBrokerReplicaActions() {
      List<ExecutionTask> interBrokerReplicaActionsToReexecute =
          new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(INTER_BROKER_REPLICA_ACTION)));
      if (!ExecutionUtils.isSubset(ExecutorUtils.partitionsBeingReassigned(_kafkaZkClient), interBrokerReplicaActionsToReexecute)) {
        LOG.info("Reexecuting tasks {}", interBrokerReplicaActionsToReexecute);
        ExecutorUtils.executeReplicaReassignmentTasks(_kafkaZkClient, interBrokerReplicaActionsToReexecute);
      }
      return !interBrokerReplicaActionsToReexecute.isEmpty();
    }

    private boolean maybeReexecuteIntraBrokerReplicaActions() {
      List<ExecutionTask> intraBrokerReplicaActionsToReexecute =
          new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(INTRA_BROKER_REPLICA_ACTION)));
      getLogdirInfoForExecutionTask(intraBrokerReplicaActionsToReexecute, _adminClient, _config).forEach((k, v) -> {
        String targetLogdir = k.proposal().replicasToMoveBetweenDisksByBroker().get(k.brokerId()).logdir();
        // If task is completed or in-progress, do not reexecute the task.
        if (targetLogdir.equals(v.getCurrentReplicaLogDir()) || targetLogdir.equals(v.getFutureReplicaLogDir())) {
          intraBrokerReplicaActionsToReexecute.remove(k);
        }
      });
      if (!intraBrokerReplicaActionsToReexecute.isEmpty()) {
        LOG.info("Reexecuting tasks {}", intraBrokerReplicaActionsToReexecute);
        executeIntraBrokerReplicaMovements(intraBrokerReplicaActionsToReexecute, _adminClient, _executionTaskManager, _config);
      }
      return !intraBrokerReplicaActionsToReexecute.isEmpty();
    }

    /**
     * Due to the race condition between the controller and Cruise Control, some of the submitted tasks may be
     * deleted by controller without being executed. We will resubmit those tasks in that case.
     */
    private void maybeReexecuteTasks() {
      boolean hasOngoingInterBrokerReplicaActions = maybeReexecuteInterBrokerReplicaActions();
      boolean hasOngoingIntraBrokerReplicaActions = maybeReexecuteIntraBrokerReplicaActions();

      // Only reexecute leader actions if there is no replica actions running.
      if (!hasOngoingInterBrokerReplicaActions && !hasOngoingIntraBrokerReplicaActions && !hasOngoingLeaderElection()) {
        List<ExecutionTask> leaderActionsToReexecute = new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(LEADER_ACTION)));
        if (!leaderActionsToReexecute.isEmpty()) {
          LOG.info("Reexecuting tasks {}", leaderActionsToReexecute);
          ExecutorUtils.executePreferredLeaderElection(_kafkaZkClient, leaderActionsToReexecute);
        }
      }
    }
  }
}
