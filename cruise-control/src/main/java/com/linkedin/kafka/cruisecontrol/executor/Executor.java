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
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.List;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.currentUtcDate;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.OPERATION_LOGGER;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorAdminUtils.*;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

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
  private static final long EXECUTION_HISTORY_SCANNER_PERIOD_SECONDS = 5;
  private static final long EXECUTION_HISTORY_SCANNER_INITIAL_DELAY_SECONDS = 0;
  // The maximum time to wait for a leader movement to finish. A leader movement will be marked as failed if
  // it takes longer than this time to finish.
  private static final long LEADER_ACTION_TIMEOUT_MS = 180000L;
  private static final String ZK_EXECUTOR_METRIC_GROUP = "CruiseControlExecutor";
  private static final String ZK_EXECUTOR_METRIC_TYPE = "Executor";
  // The execution progress is controlled by the ExecutionTaskManager.
  private final ExecutionTaskManager _executionTaskManager;
  private final MetadataClient _metadataClient;
  private final long _statusCheckingIntervalMs;
  private final ExecutorService _proposalExecutor;
  private final KafkaZkClient _kafkaZkClient;
  private final AdminClient _adminClient;

  private static final long METADATA_REFRESH_BACKOFF = 100L;
  private static final long METADATA_EXPIRY_MS = Long.MAX_VALUE;

  // Some state for external service to query
  private final AtomicBoolean _stopRequested;
  private final Time _time;
  private volatile boolean _hasOngoingExecution;
  private volatile ExecutorState _executorState;
  private volatile String _uuid;
  private final ExecutorNotifier _executorNotifier;

  private AtomicInteger _numExecutionStopped;
  private AtomicInteger _numExecutionStoppedByUser;
  private AtomicBoolean _executionStoppedByUser;
  private AtomicInteger _numExecutionStartedInKafkaAssignerMode;
  private AtomicInteger _numExecutionStartedInNonKafkaAssignerMode;
  private volatile boolean _isKafkaAssignerMode;

  private static final String EXECUTION_STARTED = "execution-started";
  private static final String KAFKA_ASSIGNER_MODE = "kafka_assigner";
  private static final String EXECUTION_STOPPED = "execution-stopped";

  private static final String GAUGE_EXECUTION_STOPPED = EXECUTION_STOPPED;
  private static final String GAUGE_EXECUTION_STOPPED_BY_USER = EXECUTION_STOPPED + "-by-user";
  private static final String GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-" + KAFKA_ASSIGNER_MODE;
  private static final String GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-non-" + KAFKA_ASSIGNER_MODE;
  // TODO: Execution history is currently kept in memory, but ideally we should move it to a persistent store.
  private final long _demotionHistoryRetentionTimeMs;
  private final long _removalHistoryRetentionTimeMs;
  private final ConcurrentMap<Integer, Long> _latestDemoteStartTimeMsByBrokerId;
  private final ConcurrentMap<Integer, Long> _latestRemoveStartTimeMsByBrokerId;
  private final ScheduledExecutorService _executionHistoryScannerExecutor;
  private UserTaskManager _userTaskManager;
  private final AnomalyDetector _anomalyDetector;

  /**
   * The executor class that execute the proposals generated by optimizer.
   *
   * @param config The configurations for Cruise Control.
   */
  public Executor(KafkaCruiseControlConfig config,
                  Time time,
                  MetricRegistry dropwizardMetricRegistry,
                  long demotionHistoryRetentionTimeMs,
                  long removalHistoryRetentionTimeMs,
                  AnomalyDetector anomalyDetector) {
    this(config, time, dropwizardMetricRegistry, null, demotionHistoryRetentionTimeMs, removalHistoryRetentionTimeMs,
         null, null, anomalyDetector);
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
           long demotionHistoryRetentionTimeMs,
           long removalHistoryRetentionTimeMs,
           ExecutorNotifier executorNotifier,
           UserTaskManager userTaskManager,
           AnomalyDetector anomalyDetector) {
      String zkUrl = config.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    _numExecutionStopped = new AtomicInteger(0);
    _numExecutionStoppedByUser = new AtomicInteger(0);
    _executionStoppedByUser = new AtomicBoolean(false);
    _numExecutionStartedInKafkaAssignerMode = new AtomicInteger(0);
    _numExecutionStartedInNonKafkaAssignerMode = new AtomicInteger(0);
    _isKafkaAssignerMode = false;
    // Register gauge sensors.
    registerGaugeSensors(dropwizardMetricRegistry);

    _time = time;
    boolean zkSecurityEnabled = config.getBoolean(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    _kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zkUrl, ZK_EXECUTOR_METRIC_GROUP, ZK_EXECUTOR_METRIC_TYPE,
        zkSecurityEnabled);
    _adminClient = KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config));
    _executionTaskManager =
        new ExecutionTaskManager(config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG),
                                 config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG),
                                 config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG),
                                 config.getList(KafkaCruiseControlConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG),
                                 _adminClient,
                                 dropwizardMetricRegistry,
                                 time);
    _metadataClient = metadataClient != null ? metadataClient
                                             : new MetadataClient(config,
                                                                  new Metadata(METADATA_REFRESH_BACKOFF, METADATA_EXPIRY_MS, false),
                                                                  -1L,
                                                                  time);
    _statusCheckingIntervalMs = config.getLong(KafkaCruiseControlConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    _proposalExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("ProposalExecutor", false, LOG));
    _latestDemoteStartTimeMsByBrokerId = new ConcurrentHashMap<>();
    _latestRemoveStartTimeMsByBrokerId = new ConcurrentHashMap<>();
    _executorState = ExecutorState.noTaskInProgress(recentlyDemotedBrokers(), recentlyRemovedBrokers());
    _stopRequested = new AtomicBoolean(false);
    _hasOngoingExecution = false;
    _uuid = null;
    _executorNotifier = executorNotifier != null ? executorNotifier
                                                 : config.getConfiguredInstance(KafkaCruiseControlConfig.EXECUTOR_NOTIFIER_CLASS_CONFIG,
                                                                                ExecutorNotifier.class);
    _userTaskManager = userTaskManager;
    _anomalyDetector = anomalyDetector;
    _demotionHistoryRetentionTimeMs = demotionHistoryRetentionTimeMs;
    _removalHistoryRetentionTimeMs = removalHistoryRetentionTimeMs;
    _executionHistoryScannerExecutor = Executors.newSingleThreadScheduledExecutor(
        new KafkaCruiseControlThreadFactory("ExecutionHistoryScanner", true, null));
    _executionHistoryScannerExecutor.scheduleAtFixedRate(new ExecutionHistoryScanner(),
                                                         EXECUTION_HISTORY_SCANNER_INITIAL_DELAY_SECONDS,
                                                         EXECUTION_HISTORY_SCANNER_PERIOD_SECONDS,
                                                         TimeUnit.SECONDS);
  }

  /**
   * Register gauge sensors.
   */
  private void registerGaugeSensors(MetricRegistry dropwizardMetricRegistry) {
    String metricName = "Executor";
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_EXECUTION_STOPPED),
                                      (Gauge<Integer>) this::numExecutionStopped);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_EXECUTION_STOPPED_BY_USER),
                                      (Gauge<Integer>) this::numExecutionStoppedByUser);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) this::numExecutionStartedInKafkaAssignerMode);
    dropwizardMetricRegistry.register(MetricRegistry.name(metricName, GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) this::numExecutionStartedInNonKafkaAssignerMode);
  }

  private void removeExpiredDemotionHistory() {
    LOG.debug("Remove expired demotion history");
    _latestDemoteStartTimeMsByBrokerId.entrySet().removeIf(entry -> (entry.getValue() + _demotionHistoryRetentionTimeMs
                                                                     < _time.milliseconds()));
  }

  private void removeExpiredRemovalHistory() {
    LOG.debug("Remove expired broker removal history");
    _latestRemoveStartTimeMsByBrokerId.entrySet().removeIf(entry -> (entry.getValue() + _removalHistoryRetentionTimeMs
                                                                     < _time.milliseconds()));
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
   * Recently demoted brokers are the ones for which a demotion was started, regardless of how the process was completed.
   *
   * @return IDs of recently demoted brokers -- i.e. demoted within the last {@link #_demotionHistoryRetentionTimeMs}.
   */
  public Set<Integer> recentlyDemotedBrokers() {
    return Collections.unmodifiableSet(_latestDemoteStartTimeMsByBrokerId.keySet());
  }

  /**
   * Recently removed brokers are the ones for which a removal was started, regardless of how the process was completed.
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
   * Check whether the executor is executing a set of proposals.
   */
  public ExecutorState state() {
    return _executorState;
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
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing a proposal (if null, no throttling is applied).
   * @param uuid UUID of the execution.
   */
  public synchronized void executeProposals(Collection<ExecutionProposal> proposals,
                                            Set<Integer> unthrottledBrokers,
                                            Set<Integer> removedBrokers,
                                            LoadMonitor loadMonitor,
                                            Integer requestedInterBrokerPartitionMovementConcurrency,
                                            Integer requestedIntraBrokerPartitionMovementConcurrency,
                                            Integer requestedLeadershipMovementConcurrency,
                                            ReplicaMovementStrategy replicaMovementStrategy,
                                            Long replicationThrottle,
                                            String uuid) {
    initProposalExecution(proposals, unthrottledBrokers, loadMonitor, requestedInterBrokerPartitionMovementConcurrency,
                          requestedIntraBrokerPartitionMovementConcurrency, requestedLeadershipMovementConcurrency,
                          replicaMovementStrategy, uuid);
    startExecution(loadMonitor, null, removedBrokers, replicationThrottle);
  }

  private synchronized void initProposalExecution(Collection<ExecutionProposal> proposals,
                                                  Collection<Integer> brokersToSkipConcurrencyCheck,
                                                  LoadMonitor loadMonitor,
                                                  Integer requestedInterBrokerPartitionMovementConcurrency,
                                                  Integer requestedIntraBrokerPartitionMovementConcurrency,
                                                  Integer requestedLeadershipMovementConcurrency,
                                                  ReplicaMovementStrategy replicaMovementStrategy,
                                                  String uuid) {
    if (_hasOngoingExecution) {
      throw new IllegalStateException("Cannot execute new proposals while there is an ongoing execution.");
    }

    if (loadMonitor == null) {
      throw new IllegalArgumentException("Load monitor cannot be null.");
    }
    if (uuid == null) {
      throw new IllegalStateException("UUID of the execution cannot be null.");
    }
    _executionTaskManager.setExecutionModeForTaskTracker(_isKafkaAssignerMode);
    _executionTaskManager.addExecutionProposals(proposals, brokersToSkipConcurrencyCheck, _metadataClient.refreshMetadata().cluster(),
                                                replicaMovementStrategy);
    setRequestedInterBrokerPartitionMovementConcurrency(requestedInterBrokerPartitionMovementConcurrency);
    setRequestedIntraBrokerPartitionMovementConcurrency(requestedIntraBrokerPartitionMovementConcurrency);
    setRequestedLeadershipMovementConcurrency(requestedLeadershipMovementConcurrency);
    _uuid = uuid;
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
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            while executing demotion proposals (if null, no throttling is applied).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   * @param uuid UUID of the execution.
   */
  public synchronized void executeDemoteProposals(Collection<ExecutionProposal> proposals,
                                                  Collection<Integer> demotedBrokers,
                                                  LoadMonitor loadMonitor,
                                                  Integer concurrentSwaps,
                                                  Integer requestedLeadershipMovementConcurrency,
                                                  ReplicaMovementStrategy replicaMovementStrategy,
                                                  Long replicationThrottle,
                                                  String uuid) {
    initProposalExecution(proposals, demotedBrokers, loadMonitor, concurrentSwaps, 0, requestedLeadershipMovementConcurrency,
                          replicaMovementStrategy, uuid);
    startExecution(loadMonitor, demotedBrokers, null, replicationThrottle);
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
  public synchronized void setExecutionMode(boolean isKafkaAssignerMode) {
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
   */
  private void startExecution(LoadMonitor loadMonitor,
                              Collection<Integer> demotedBrokers,
                              Collection<Integer> removedBrokers,
                              Long replicationThrottle) {
    _executionStoppedByUser.set(false);
    sanityCheckOngoingReplicaMovement();
    _hasOngoingExecution = true;
    _stopRequested.set(false);
    _executionStoppedByUser.set(false);
    if (_isKafkaAssignerMode) {
      _numExecutionStartedInKafkaAssignerMode.incrementAndGet();
    } else {
      _numExecutionStartedInNonKafkaAssignerMode.incrementAndGet();
    }
    _proposalExecutor.submit(
            new ProposalExecutionRunnable(loadMonitor, demotedBrokers, removedBrokers, replicationThrottle));
  }

  /**
   * Sanity check whether there are ongoing inter-broker or intra-broker replica movements.
   */
  private void sanityCheckOngoingReplicaMovement() {
    // Note that in case there is an ongoing partition reassignment, we do not unpause metric sampling.
    if (hasOngoingPartitionReassignments()) {
      _executionTaskManager.clear();
      _uuid = null;
      throw new IllegalStateException("There are ongoing inter-broker partition movements.");
    }

    if (isOngoingIntraBrokerReplicaMovement(_metadataClient.cluster().nodes().stream().mapToInt(Node::id).boxed()
                                                           .collect(Collectors.toSet()),
                                            _adminClient)) {
      _executionTaskManager.clear();
      _uuid = null;
      throw new IllegalStateException("There are ongoing intra-broker partition movements.");
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   */
  public synchronized void userTriggeredStopExecution() {
    if (stopExecution()) {
      LOG.info("User requested to stop the ongoing proposal execution.");
      _numExecutionStoppedByUser.incrementAndGet();
      _executionStoppedByUser.set(true);
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   *
   * @return True if the flag to stop the execution is set after the call (i.e. was not set already), false otherwise.
   */
  private synchronized boolean stopExecution() {
    if (_stopRequested.compareAndSet(false, true)) {
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
    LOG.info("Executor shutdown completed.");
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
   * Whether there is any ongoing partition reassignment.
   * This method directly checks the existence of znode /admin/partition_reassignment.
   * Note this method returning false does not guarantee that there is no ongoing execution because when there is an ongoing
   * execution inside Cruise Control, partition reassignment task batches are writen to zookeeper periodically, there will be
   * small intervals that /admin/partition_reassignment does not exist.
   *
   * @return True if there is any ongoing partition reassignment.
   */
  public boolean hasOngoingPartitionReassignments() {
    return !ExecutorUtils.partitionsBeingReassigned(_kafkaZkClient).isEmpty();
  }

  /**
   * This class is thread safe.
   *
   * Note that once the thread for {@link ProposalExecutionRunnable} is submitted for running, the variable
   * _executionTaskManager can only be written within this inner class, but not from the outer Executor class.
   */
  private class ProposalExecutionRunnable implements Runnable {
    private final LoadMonitor _loadMonitor;
    private ExecutorState.State _state;
    private Set<Integer> _recentlyDemotedBrokers;
    private Set<Integer> _recentlyRemovedBrokers;
    private final Long _replicationThrottle;
    private final long _executionStartMs;
    private Throwable _executionException;

    ProposalExecutionRunnable(LoadMonitor loadMonitor,
                              Collection<Integer> demotedBrokers,
                              Collection<Integer> removedBrokers,
                              Long replicationThrottle) {
      _loadMonitor = loadMonitor;
      _state = NO_TASK_IN_PROGRESS;
      _executionStartMs = _time.milliseconds();
      _executionException = null;

      if (_userTaskManager == null) {
        throw new IllegalStateException("UserTaskManager is not specified in Executor.");
      }
      if (_anomalyDetector == null) {
        throw new IllegalStateException("AnomalyDetector is not specified in Executor.");
      }

      if (demotedBrokers != null) {
        // Add/overwrite the latest demotion time of demoted brokers (if any).
        demotedBrokers.forEach(id -> _latestDemoteStartTimeMsByBrokerId.put(id, _time.milliseconds()));
      }
      if (removedBrokers != null) {
        // Add/overwrite the latest removal time of removed brokers (if any).
        removedBrokers.forEach(id -> _latestRemoveStartTimeMsByBrokerId.put(id, _time.milliseconds()));
      }
      _recentlyDemotedBrokers = recentlyDemotedBrokers();
      _recentlyRemovedBrokers = recentlyRemovedBrokers();
      _replicationThrottle = replicationThrottle;
    }

    public void run() {
      LOG.info("Starting executing balancing proposals.");
      execute();
      LOG.info("Execution finished.");
    }

    /**
     * Start the actual execution of the proposals in order: First move replicas, then transfer leadership.
     */
    private void execute() {
      UserTaskManager.UserTaskInfo userTaskInfo;
      // If the task is triggered from a user request, mark the task to be in-execution state in user task manager and
      // retrieve the associated user task information.
      if (AnomalyType.cachedValues().stream().anyMatch(type -> _uuid.startsWith(type.toString()))) {
        userTaskInfo = null;
      } else {
        userTaskInfo = _userTaskManager.markTaskExecutionBegan(_uuid);
      }
      _state = STARTING_EXECUTION;
      _executorState = ExecutorState.executionStarted(_uuid, _recentlyDemotedBrokers, _recentlyRemovedBrokers);
      OPERATION_LOG.info("Task [{}] execution starts.", _uuid);
      try {
        // Pause the metric sampling to avoid the loss of accuracy during execution.
        while (true) {
          try {
            // Ensure that the temporary states in the load monitor are explicitly handled -- e.g. SAMPLING.
            _loadMonitor.pauseMetricSampling(String.format("Paused-By-Cruise-Control-Before-Starting-Execution (Date: %s)", currentUtcDate()));
            break;
          } catch (IllegalStateException e) {
            Thread.sleep(_statusCheckingIntervalMs);
            LOG.debug("Waiting for the load monitor to be ready to initialize the execution.", e);
          }
        }

        // 1. Inter-broker move replicas if possible.
        if (_state == STARTING_EXECUTION) {
          _state = INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS;
          // The _executorState might be inconsistent with _state if the user checks it between the two assignments.
          _executorState = ExecutorState.operationInProgress(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(INTER_BROKER_REPLICA_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers);
          interBrokerMoveReplicas();
          updateOngoingExecutionState();
        }

        // 2. Intra-broker move replicas if possible.
        if (_state == INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          _state = INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS;
          // The _executorState might be inconsistent with _state if the user checks it between the two assignments.
          _executorState = ExecutorState.operationInProgress(INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers);
          intraBrokerMoveReplicas();
          updateOngoingExecutionState();
        }

        // 3. Transfer leadership if possible.
        if (_state == INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          _state = LEADER_MOVEMENT_TASK_IN_PROGRESS;
          // The _executorState might be inconsistent with _state if the user checks it between the two assignments.
          _executorState = ExecutorState.operationInProgress(LEADER_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(LEADER_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers);
          moveLeaderships();
          updateOngoingExecutionState();
        }
      } catch (Throwable t) {
        LOG.error("Executor got exception during execution", t);
        _executionException = t;
      } finally {
        // If the finished task was triggered by a user request, update task status in user task manager; if task is triggered
        // by an anomaly self-healing, update the task status in anomaly detector.
        if (userTaskInfo != null) {
          _userTaskManager.markTaskExecutionFinished(_uuid);
        } else {
          _anomalyDetector.markSelfHealingFinished(_uuid);
        }
        _loadMonitor.resumeMetricSampling(String.format("Resumed-By-Cruise-Control-After-Completed-Execution (Date: %s)", currentUtcDate()));

        // If execution encountered exception and isn't stopped, it's considered successful.
        boolean executionSucceeded = _executorState.state() != STOPPING_EXECUTION && _executionException == null;
        // If we are here, either we succeeded, or we are stopped or had exception. Send notification to user.
        ExecutorNotification notification = new ExecutorNotification(_executionStartMs, _time.milliseconds(),
                                                                     userTaskInfo, _uuid, _stopRequested.get(),
                                                                     _executionStoppedByUser.get(),
                                                                     _executionException, executionSucceeded);
        _executorNotifier.sendNotification(notification);
        OPERATION_LOG.info("Task [{}] execution finishes.", _uuid);
        // Clear completed execution.
        clearCompletedExecution();
      }
    }

    private void clearCompletedExecution() {
      _executionTaskManager.clear();
      _uuid = null;
      _state = NO_TASK_IN_PROGRESS;
      // The _executorState might be inconsistent with _state if the user checks it between the two assignments.
      _executorState = ExecutorState.noTaskInProgress(_recentlyDemotedBrokers, _recentlyRemovedBrokers);
      _hasOngoingExecution = false;
      _stopRequested.set(false);
      _executionStoppedByUser.set(false);
    }

    private void updateOngoingExecutionState() {
      if (!_stopRequested.get()) {
        switch (_state) {
          case LEADER_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(LEADER_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(LEADER_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers);
            break;
          case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(INTER_BROKER_REPLICA_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers);
            break;
          case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers);
            break;
          default:
            throw new IllegalStateException("Unexpected ongoing execution state " + _state);
        }
      } else {
        _state = ExecutorState.State.STOPPING_EXECUTION;
        _executorState = ExecutorState.operationInProgress(STOPPING_EXECUTION,
                                                           _executionTaskManager.getExecutionTasksSummary(new HashSet<>(ExecutionTask.TaskType.cachedValues())),
                                                           _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                           _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                           _executionTaskManager.leadershipMovementConcurrency(),
                                                           _uuid,
                                                           _recentlyDemotedBrokers,
                                                           _recentlyRemovedBrokers);
      }
    }

    private void interBrokerMoveReplicas() {
      ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_kafkaZkClient, _replicationThrottle);
      int numTotalPartitionMovements = _executionTaskManager.numRemainingInterBrokerPartitionMovements();
      long totalDataToMoveInMB = _executionTaskManager.remainingInterBrokerDataToMoveInMB();
      LOG.info("Starting {} inter-broker partition movements.", numTotalPartitionMovements);

      int partitionsToMove = numTotalPartitionMovements;
      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || !_executionTaskManager.inExecutionTasks().isEmpty()) && !_stopRequested.get()) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getInterBrokerReplicaMovementTasks();
        LOG.info("Executor will execute {} task(s)", tasksToExecute.size());

        if (!tasksToExecute.isEmpty()) {
          throttleHelper.setThrottles(
              tasksToExecute.stream().map(ExecutionTask::proposal).collect(Collectors.toList()));
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
                 String.format(java.util.Locale.US, "%.2f",
                               numFinishedPartitionMovements * 100.0 / numTotalPartitionMovements),
                 finishedDataMovementInMB, totalDataToMoveInMB,
                 totalDataToMoveInMB == 0 ? 100 : String.format(java.util.Locale.US, "%.2f",
                                                  (finishedDataMovementInMB * 100.0) / totalDataToMoveInMB));
        throttleHelper.clearThrottles(completedTasks, tasksToExecute.stream().filter(t -> t.state() == IN_PROGRESS).collect(Collectors.toList()));
      }
      // After the partition movement finishes, wait for the controller to clean the reassignment zkPath. This also
      // ensures a clean stop when the execution is stopped in the middle.
      Set<ExecutionTask> inExecutionTasks = _executionTaskManager.inExecutionTasks();
      while (!inExecutionTasks.isEmpty()) {
        LOG.info("Waiting for {} tasks moving {} MB to finish.", inExecutionTasks.size(),
                 _executionTaskManager.inExecutionInterBrokerDataToMoveInMB());
        List<ExecutionTask> completedRemainingTasks = waitForExecutionTaskToFinish();
        inExecutionTasks = _executionTaskManager.inExecutionTasks();
        throttleHelper.clearThrottles(completedRemainingTasks, new ArrayList<>(inExecutionTasks));
      }
      if (_executionTaskManager.inExecutionTasks().isEmpty()) {
        LOG.info("Inter-broker partition movements finished.");
      } else if (_stopRequested.get()) {
        ExecutionTasksSummary executionTasksSummary = _executionTaskManager.getExecutionTasksSummary(Collections.emptySet());
        Map<ExecutionTask.State, Integer> partitionMovementTasksByState = executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
        LOG.info("Inter-broker partition movements stopped. For inter-broker partition movements {} tasks cancelled, {} tasks in-progress, "
                 + "{} tasks aborting, {} tasks aborted, {} tasks dead, {} tasks completed, {} remaining data to move; for intra-broker "
                 + "partition movement {} tasks cancelled; for leadership movements {} task cancelled.",
                 partitionMovementTasksByState.get(PENDING),
                 partitionMovementTasksByState.get(IN_PROGRESS),
                 partitionMovementTasksByState.get(ABORTING),
                 partitionMovementTasksByState.get(ABORTED),
                 partitionMovementTasksByState.get(DEAD),
                 partitionMovementTasksByState.get(COMPLETED),
                 executionTasksSummary.remainingInterBrokerDataToMoveInMB(),
                 executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION).get(PENDING),
                 executionTasksSummary.taskStat().get(LEADER_ACTION).get(PENDING));
      }
    }

    private void intraBrokerMoveReplicas() {
      int numTotalPartitionMovements = _executionTaskManager.numRemainingIntraBrokerPartitionMovements();
      long totalDataToMoveInMB = _executionTaskManager.remainingIntraBrokerDataToMoveInMB();
      LOG.info("Starting {} intra-broker partition movements.", numTotalPartitionMovements);

      int partitionsToMove = numTotalPartitionMovements;
      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || !_executionTaskManager.inExecutionTasks().isEmpty()) && !_stopRequested.get()) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getIntraBrokerReplicaMovementTasks();
        LOG.info("Executor will execute {} task(s)", tasksToExecute.size());

        if (!tasksToExecute.isEmpty()) {
          // Execute the tasks.
          _executionTaskManager.markTasksInProgress(tasksToExecute);
          executeIntraBrokerReplicaMovements(tasksToExecute, _adminClient, _executionTaskManager);
        }
        // Wait indefinitely for partition movements to finish.
        waitForExecutionTaskToFinish();
        partitionsToMove = _executionTaskManager.numRemainingIntraBrokerPartitionMovements();
        int numFinishedPartitionMovements = _executionTaskManager.numFinishedIntraBrokerPartitionMovements();
        long finishedDataToMoveInMB = _executionTaskManager.finishedIntraBrokerDataToMoveInMB();
        LOG.info("{}/{} ({}%) intra-broker partition movements completed. {}/{} ({}%) MB have been moved.",
            numFinishedPartitionMovements, numTotalPartitionMovements,
            String.format(java.util.Locale.US, "%.2f",
                          numFinishedPartitionMovements * 100.0 / numTotalPartitionMovements),
            finishedDataToMoveInMB, totalDataToMoveInMB,
            totalDataToMoveInMB == 0 ? 100 : String.format(java.util.Locale.US, "%.2f",
                                                           (finishedDataToMoveInMB * 100.0) / totalDataToMoveInMB));
      }
      Set<ExecutionTask> inExecutionTasks = _executionTaskManager.inExecutionTasks();
      while (!inExecutionTasks.isEmpty()) {
        LOG.info("Waiting for {} tasks moving {} MB to finish", inExecutionTasks.size(),
                 _executionTaskManager.inExecutionIntraBrokerDataMovementInMB(), inExecutionTasks);
        waitForExecutionTaskToFinish();
        inExecutionTasks = _executionTaskManager.inExecutionTasks();
      }
      if (_executionTaskManager.inExecutionTasks().isEmpty()) {
        LOG.info("Intra-broker partition movements finished.");
      } else if (_stopRequested.get()) {
        ExecutionTasksSummary executionTasksSummary = _executionTaskManager.getExecutionTasksSummary(Collections.emptySet());
        Map<ExecutionTask.State, Integer> partitionMovementTasksByState = executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
        LOG.info("Intra-broker partition movements stopped. For intra-broker partition movements {} tasks cancelled, {} tasks in-progress, "
                 + "{} tasks aborting, {} tasks aborted, {} tasks dead, {} tasks completed, {} remaining data to move; for leadership "
                 + "movements {} task cancelled.",
                 partitionMovementTasksByState.get(PENDING),
                 partitionMovementTasksByState.get(IN_PROGRESS),
                 partitionMovementTasksByState.get(ABORTING),
                 partitionMovementTasksByState.get(ABORTED),
                 partitionMovementTasksByState.get(DEAD),
                 partitionMovementTasksByState.get(COMPLETED),
                 executionTasksSummary.remainingIntraBrokerDataToMoveInMB(),
                 executionTasksSummary.taskStat().get(LEADER_ACTION).get(PENDING));
      }
    }

    private void moveLeaderships() {
      int numTotalLeadershipMovements = _executionTaskManager.numRemainingLeadershipMovements();
      LOG.info("Starting {} leadership movements.", numTotalLeadershipMovements);
      int numFinishedLeadershipMovements = 0;
      while (_executionTaskManager.numRemainingLeadershipMovements() != 0 && !_stopRequested.get()) {
        updateOngoingExecutionState();
        numFinishedLeadershipMovements += moveLeadershipInBatch();
        LOG.info("{}/{} ({}%) leadership movements completed.", numFinishedLeadershipMovements,
                 numTotalLeadershipMovements, numFinishedLeadershipMovements * 100 / numTotalLeadershipMovements);
      }
      if (_executionTaskManager.inExecutionTasks().isEmpty()) {
        LOG.info("Leadership movements finished.");
      } else if (_stopRequested.get()) {
        Map<ExecutionTask.State, Integer> leadershipMovementTasksByState =
            _executionTaskManager.getExecutionTasksSummary(Collections.emptySet()).taskStat().get(LEADER_ACTION);
        LOG.info("Leadership movements stopped. {} tasks cancelled, {} tasks in-progress, {} tasks aborting, {} tasks aborted, "
                 + "{} tasks dead, {} tasks completed.",
                 leadershipMovementTasksByState.get(PENDING),
                 leadershipMovementTasksByState.get(IN_PROGRESS),
                 leadershipMovementTasksByState.get(ABORTING),
                 leadershipMovementTasksByState.get(ABORTED),
                 leadershipMovementTasksByState.get(DEAD),
                 leadershipMovementTasksByState.get(COMPLETED));
      }
    }

    private int moveLeadershipInBatch() {
      List<ExecutionTask> leadershipMovementTasks = _executionTaskManager.getLeadershipMovementTasks();
      int numLeadershipToMove = leadershipMovementTasks.size();
      LOG.debug("Executing {} leadership movements in a batch.", numLeadershipToMove);
      // Execute the leadership movements.
      if (!leadershipMovementTasks.isEmpty() && !_stopRequested.get()) {
        // Mark leadership movements in progress.
        _executionTaskManager.markTasksInProgress(leadershipMovementTasks);

        // Run preferred leader election.
        ExecutorUtils.executePreferredLeaderElection(_kafkaZkClient, leadershipMovementTasks);
        LOG.trace("Waiting for leadership movement batch to finish.");
        while (!_executionTaskManager.inExecutionTasks().isEmpty() && !_stopRequested.get()) {
          waitForExecutionTaskToFinish();
        }
      }
      return numLeadershipToMove;
    }

    /**
     * This method periodically check zookeeper to see if the partition reassignment has finished or not.
     */
    private List<ExecutionTask> waitForExecutionTaskToFinish() {
      List<ExecutionTask> finishedTasks = new ArrayList<>();
      do {
        // If there is no finished tasks, we need to check if anything is blocked.
        maybeReexecuteTasks();
        try {
          Thread.sleep(_statusCheckingIntervalMs);
        } catch (InterruptedException e) {
          // let it go
        }

        Cluster cluster = _metadataClient.refreshMetadata().cluster();
        Map<ExecutionTask, ReplicaLogDirInfo> logDirInfoByTask = getLogdirInfoForExecutionTask(
            _executionTaskManager.inExecutionTasks(Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
            _adminClient);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Tasks in execution: {}", _executionTaskManager.inExecutionTasks());
        }
        List<ExecutionTask> deadOrAbortingTasks = new ArrayList<>();
        for (ExecutionTask task : _executionTaskManager.inExecutionTasks()) {
          TopicPartition tp = task.proposal().topicPartition();
          if (cluster.partition(tp) == null) {
            // Handle topic deletion during the execution.
            LOG.debug("Task {} is marked as finished because the topic has been deleted", task);
            finishedTasks.add(task);
            _executionTaskManager.markTaskAborting(task);
            _executionTaskManager.markTaskDone(task);
          } else if (isTaskDone(cluster, logDirInfoByTask, tp, task)) {
            // Check to see if the task is done.
            finishedTasks.add(task);
            _executionTaskManager.markTaskDone(task);
          } else if (maybeMarkTaskAsDeadOrAborting(cluster, logDirInfoByTask, task)) {
            // Only add the dead or aborted tasks to execute if it is not a leadership movement.
            if (task.type() != LEADER_ACTION) {
              deadOrAbortingTasks.add(task);
            }
            // A dead or aborted task is considered as finished.
            if (task.state() == DEAD || task.state() == ABORTED) {
              finishedTasks.add(task);
            }
          }
        }
        // TODO: Execute the dead or aborted tasks.
        if (!deadOrAbortingTasks.isEmpty()) {
          // TODO: re-enable this rollback action when KAFKA-6304 is available.
          // ExecutorAdminUtils.executeReplicaReassignmentTasks(_kafkaZkClient, deadOrAbortingTasks);
          if (!_stopRequested.get()) {
            // If there is task aborted or dead, we stop the execution.
            stopExecution();
          }
        }
        updateOngoingExecutionState();
      } while (!_executionTaskManager.inExecutionTasks().isEmpty() && finishedTasks.isEmpty());
      LOG.info("Completed tasks: {}", finishedTasks);
      return finishedTasks;
    }

    /**
     * Check if a task is done.
     */
    private boolean isTaskDone(Cluster cluster,
                               Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask,
                               TopicPartition  tp,
                               ExecutionTask task) {
      switch (task.type()) {
        case INTER_BROKER_REPLICA_ACTION:
          return isInterBrokerReplicaActionDone(cluster, tp, task);
        case INTRA_BROKER_REPLICA_ACTION:
          return isIntraBrokerReplicaActionDone(logdirInfoByTask, task);
        case LEADER_ACTION:
          return isLeadershipMovementDone(cluster, tp, task);
        default:
          return true;
      }
    }

    /**
     * For a inter-broker replica movement action, the completion depends on the task state:
     * IN_PROGRESS: when the current replica list is the same as the new replica list.
     * ABORTING: done when the current replica list is the same as the old replica list. Due to race condition,
     *           we also consider it done if the current replica list is the same as the new replica list.
     * DEAD: always considered as done because we neither move forward or rollback.
     *
     * There should be no other task state seen here.
     */
    private boolean isInterBrokerReplicaActionDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      Node[] currentOrderedReplicas = cluster.partition(tp).replicas();
      switch (task.state()) {
        case IN_PROGRESS:
          return task.proposal().isInterBrokerMovementCompleted(currentOrderedReplicas);
        case ABORTING:
          return task.proposal().isInterBrokerMovementAborted(currentOrderedReplicas);
        case DEAD:
          return true;
        default:
          throw new IllegalStateException("Should never be here. State " + task.state());
      }
    }

    /**
     * Check whether intra-broker replica movement is done by comparing replica's current logdir with the logdir proposed
     * by task's proposal.
     */
    private boolean isIntraBrokerReplicaActionDone(Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask,
                                                   ExecutionTask task) {
      if (logdirInfoByTask.containsKey(task)) {
        return logdirInfoByTask.get(task).getCurrentReplicaLogDir()
                               .equals(task.proposal().replicasToMoveBetweenDisksByBroker().get(task.brokerId()).logdir());
      }
      return false;
    }

    private boolean isInIsr(Integer leader, Cluster cluster, TopicPartition tp) {
      return Arrays.stream(cluster.partition(tp).inSyncReplicas()).anyMatch(node -> node.id() == leader);
    }

    /**
     * The completeness of leadership movement depends on the task state:
     * IN_PROGRESS: done when the leader becomes the destination.
     * ABORTING or DEAD: always considered as done the destination cannot become leader anymore.
     *
     * There should be no other task state seen here.
     */
    private boolean isLeadershipMovementDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      Node leader = cluster.leaderFor(tp);
      switch (task.state()) {
        case IN_PROGRESS:
          return (leader != null && leader.id() == task.proposal().newLeader().brokerId())
                 || leader == null
                 || !isInIsr(task.proposal().newLeader().brokerId(), cluster, tp);
        case ABORTING:
        case DEAD:
          return true;
        default:
          throw new IllegalStateException("Should never be here.");
      }
    }

    /**
     * Mark the task as aborting or dead if needed.
     *
     * Ideally, the task should be marked as:
     * 1. ABORTING: when the execution is stopped by the users.
     * 2. ABORTING: When the destination broker&disk is dead so the task cannot make progress, but the source broker&disk
     *              is still alive.
     * 3. DEAD: when any replica in the new replica list is dead. Or when a leader action times out.
     *
     * Currently KafkaController does not support updates on the partitions that is being reassigned. (KAFKA-6304)
     * Therefore once a proposals is written to ZK, we cannot revoke it. So the actual behavior we are using is to
     * set the task state to:
     * 1. IN_PROGRESS: when the execution is stopped by the users. i.e. do nothing but let the task finish normally.
     * 2. DEAD: when the destination broker&disk is dead. i.e. do not block on the execution.
     *
     * @param cluster the kafka cluster
     * @param logdirInfoByTask  disk information for ongoing intra-broker replica movement tasks
     * @param task the task to check
     * @return true if the task is marked as dead or aborting, false otherwise.
     */
    private boolean maybeMarkTaskAsDeadOrAborting(Cluster cluster,
                                                  Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask,
                                                  ExecutionTask task) {
      // Only check tasks with IN_PROGRESS or ABORTING state.
      if (task.state() == IN_PROGRESS || task.state() == ABORTING) {
        switch (task.type()) {
          case LEADER_ACTION:
            if (cluster.nodeById(task.proposal().newLeader().brokerId()) == null) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("Killing execution for task {} because the target leader is down", task);
              return true;
            } else if (_time.milliseconds() > task.startTime() + LEADER_ACTION_TIMEOUT_MS) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("Failed task {} because it took longer than {} to finish.", task, LEADER_ACTION_TIMEOUT_MS);
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

    /**
     * Checks whether the topicPartitions of the execution tasks in the given subset is indeed a subset of the given set.
     *
     * @param set The original set.
     * @param subset The subset to validate whether it is indeed a subset of the given set.
     * @return True if the topicPartitions of the given subset constitute a subset of the given set, false otherwise.
     */
    private boolean isSubset(Set<TopicPartition> set, Collection<ExecutionTask> subset) {
      boolean isSubset = true;
      for (ExecutionTask executionTask : subset) {
        TopicPartition tp = executionTask.proposal().topicPartition();
        if (!set.contains(tp)) {
          isSubset = false;
          break;
        }
      }

      return isSubset;
    }

    /**
     * Due to the race condition between the controller and Cruise Control, some of the submitted tasks may be
     * deleted by controller without being executed. We will resubmit those tasks in that case.
     */
    private void maybeReexecuteTasks() {
      List<ExecutionTask> interBrokerReplicaActionsToReexecute =
          new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(INTER_BROKER_REPLICA_ACTION)));
      if (!isSubset(ExecutorUtils.partitionsBeingReassigned(_kafkaZkClient), interBrokerReplicaActionsToReexecute)) {
        LOG.info("Reexecuting tasks {}", interBrokerReplicaActionsToReexecute);
        ExecutorUtils.executeReplicaReassignmentTasks(_kafkaZkClient, interBrokerReplicaActionsToReexecute);
      }

      List<ExecutionTask> intraBrokerReplicaActionsToReexecute =
          new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(INTRA_BROKER_REPLICA_ACTION)));
      getLogdirInfoForExecutionTask(intraBrokerReplicaActionsToReexecute, _adminClient).forEach((k, v) -> {
        String targetLogdir = k.proposal().replicasToMoveBetweenDisksByBroker().get(k.brokerId()).logdir();
        // If task is completed or in-progess, do not reexecute the task.
        if (targetLogdir.equals(v.getCurrentReplicaLogDir()) || targetLogdir.equals(v.getFutureReplicaLogDir())) {
          intraBrokerReplicaActionsToReexecute.remove(k);
        }
      });
      if (!intraBrokerReplicaActionsToReexecute.isEmpty()) {
        LOG.info("Reexecuting tasks {}", intraBrokerReplicaActionsToReexecute);
        executeIntraBrokerReplicaMovements(intraBrokerReplicaActionsToReexecute, _adminClient, _executionTaskManager);
      }

      // Only reexecute leader actions if there is no replica actions running.
      if (interBrokerReplicaActionsToReexecute.isEmpty() &&
          intraBrokerReplicaActionsToReexecute.isEmpty() &&
          ExecutorUtils.ongoingLeaderElection(_kafkaZkClient).isEmpty()) {
        List<ExecutionTask> leaderActionsToReexecute = new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(LEADER_ACTION)));
        if (!leaderActionsToReexecute.isEmpty()) {
          LOG.info("Reexecuting tasks {}", leaderActionsToReexecute);
          ExecutorUtils.executePreferredLeaderElection(_kafkaZkClient, leaderActionsToReexecute);
        }
      }
    }
  }
}
