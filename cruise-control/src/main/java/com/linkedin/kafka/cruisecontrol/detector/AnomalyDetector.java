/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.OPERATION_LOGGER;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckGoals;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.anomalyComparator;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.SHUTDOWN_ANOMALY;


/**
 * The anomaly detector class that helps detect and handle anomalies.
 */
public class AnomalyDetector {
  static final String METRIC_REGISTRY_NAME = "AnomalyDetector";
  private static final int INIT_JITTER_BOUND = 10000;
  private static final long SCHEDULER_SHUTDOWN_TIMEOUT_MS = 5000L;
  private static final int NUM_ANOMALY_DETECTION_THREADS = 5;
  private static final int ANOMALY_QUEUE_INITIAL_CAPACITY = 10;
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
  private static final Logger OPERATION_LOG = LoggerFactory.getLogger(OPERATION_LOGGER);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final AnomalyNotifier _anomalyNotifier;
  private final AdminClient _adminClient;
  // Detectors
  private final GoalViolationDetector _goalViolationDetector;
  private final BrokerFailureDetector _brokerFailureDetector;
  private final MetricAnomalyDetector _metricAnomalyDetector;
  private final DiskFailureDetector _diskFailureDetector;
  private final ScheduledExecutorService _detectorScheduler;
  private final long _goalViolationDetectionIntervalMs;
  private final long _diskFailureDetectionIntervalMs;
  private final long _metricAnomalyDetectionIntervalMs;
  private final long _brokerFailureDetectionBackoffMs;
  private final PriorityBlockingQueue<Anomaly> _anomalies;
  private volatile boolean _shutdown;
  private final AnomalyDetectorState _anomalyDetectorState;
  private final List<String> _selfHealingGoals;
  private final ExecutorService _anomalyLoggerExecutor;
  private volatile Anomaly _anomalyInProgress;
  private volatile long _numCheckedWithDelay;
  private final Object _shutdownLock;

  public AnomalyDetector(KafkaCruiseControl kafkaCruiseControl,
                         Time time,
                         MetricRegistry dropwizardMetricRegistry) {
    // For anomalies of different types, prioritize handling anomaly of higher priority;
    // otherwise, handle anomaly in order of detected time.
    _anomalies = new PriorityBlockingQueue<>(ANOMALY_QUEUE_INITIAL_CAPACITY, anomalyComparator());
    KafkaCruiseControlConfig config = kafkaCruiseControl.config();
    _adminClient = KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config));
    long anomalyDetectionIntervalMs = config.getLong(AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    Long goalViolationDetectionIntervalMs = config.getLong(AnomalyDetectorConfig.GOAL_VIOLATION_DETECTION_INTERVAL_MS_CONFIG);
    _goalViolationDetectionIntervalMs = goalViolationDetectionIntervalMs == null ? anomalyDetectionIntervalMs
                                                                                 : goalViolationDetectionIntervalMs;
    Long metricAnomalyDetectionIntervalMs = config.getLong(AnomalyDetectorConfig.METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    _metricAnomalyDetectionIntervalMs = metricAnomalyDetectionIntervalMs == null ? anomalyDetectionIntervalMs
                                                                                 : metricAnomalyDetectionIntervalMs;
    Long diskFailureDetectionIntervalMs = config.getLong(AnomalyDetectorConfig.DISK_FAILURE_DETECTION_INTERVAL_MS_CONFIG);
    _diskFailureDetectionIntervalMs = diskFailureDetectionIntervalMs == null ? anomalyDetectionIntervalMs
                                                                             : diskFailureDetectionIntervalMs;
    _brokerFailureDetectionBackoffMs = config.getLong(AnomalyDetectorConfig.BROKER_FAILURE_DETECTION_BACKOFF_MS_CONFIG);
    _anomalyNotifier = config.getConfiguredInstance(AnomalyDetectorConfig.ANOMALY_NOTIFIER_CLASS_CONFIG,
                                                    AnomalyNotifier.class);
    _kafkaCruiseControl = kafkaCruiseControl;
    _selfHealingGoals = getSelfHealingGoalNames(config);
    sanityCheckGoals(_selfHealingGoals, false, config);
    _goalViolationDetector = new GoalViolationDetector(_anomalies, _kafkaCruiseControl);
    _brokerFailureDetector = new BrokerFailureDetector(_anomalies, _kafkaCruiseControl);
    _metricAnomalyDetector = new MetricAnomalyDetector(_anomalies, _kafkaCruiseControl);
    _diskFailureDetector = new DiskFailureDetector(_adminClient, _anomalies, _kafkaCruiseControl);
    _detectorScheduler = Executors.newScheduledThreadPool(NUM_ANOMALY_DETECTION_THREADS,
                                                          new KafkaCruiseControlThreadFactory(METRIC_REGISTRY_NAME, false, LOG));
    _shutdown = false;
    // Add anomaly detector state
    int numCachedRecentAnomalyStates = config.getInt(AnomalyDetectorConfig.NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG);
    _anomalyLoggerExecutor =
        Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("AnomalyLogger", true, null));
    _anomalyInProgress = null;
    _numCheckedWithDelay = 0L;
    _shutdownLock = new Object();
    dropwizardMetricRegistry.register(MetricRegistry.name(METRIC_REGISTRY_NAME, "balancedness-score"),
                                      (Gauge<Double>) _goalViolationDetector::balancednessScore);
    _anomalyDetectorState = new AnomalyDetectorState(time,
                                                     _anomalyNotifier.selfHealingEnabled(),
                                                     numCachedRecentAnomalyStates,
                                                     dropwizardMetricRegistry);
  }

  /**
   * Package private constructor for unit test.
   */
  AnomalyDetector(PriorityBlockingQueue<Anomaly> anomalies,
                  AdminClient adminClient,
                  long anomalyDetectionIntervalMs,
                  KafkaCruiseControl kafkaCruiseControl,
                  AnomalyNotifier anomalyNotifier,
                  GoalViolationDetector goalViolationDetector,
                  BrokerFailureDetector brokerFailureDetector,
                  MetricAnomalyDetector metricAnomalyDetector,
                  DiskFailureDetector diskFailureDetector,
                  ScheduledExecutorService detectorScheduler) {
    _anomalies = anomalies;
    _adminClient = adminClient;
    _goalViolationDetectionIntervalMs = anomalyDetectionIntervalMs;
    _metricAnomalyDetectionIntervalMs = anomalyDetectionIntervalMs;
    _diskFailureDetectionIntervalMs = anomalyDetectionIntervalMs;
    _brokerFailureDetectionBackoffMs = anomalyDetectionIntervalMs;
    _anomalyNotifier = anomalyNotifier;
    _goalViolationDetector = goalViolationDetector;
    _brokerFailureDetector = brokerFailureDetector;
    _metricAnomalyDetector = metricAnomalyDetector;
    _diskFailureDetector = diskFailureDetector;
    _kafkaCruiseControl = kafkaCruiseControl;
    _detectorScheduler = detectorScheduler;
    _shutdown = false;
    _selfHealingGoals = Collections.emptyList();
    _anomalyLoggerExecutor =
        Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("AnomalyLogger", true, null));
    _anomalyInProgress = null;
    _numCheckedWithDelay = 0L;
    _shutdownLock = new Object();
    // Add anomaly detector state
    _anomalyDetectorState = new AnomalyDetectorState(new SystemTime(), new HashMap<>(KafkaAnomalyType.cachedValues().size()), 10, null);
  }

  /**
   * Start each anomaly detector.
   */
  public void startDetection() {
    LOG.info("Starting anomaly detector.");
    _brokerFailureDetector.startDetection();
    int jitter = new Random().nextInt(INIT_JITTER_BOUND);
    LOG.debug("Starting goal violation detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_goalViolationDetector,
                                           _goalViolationDetectionIntervalMs / 2 + jitter,
                                           _goalViolationDetectionIntervalMs,
                                           TimeUnit.MILLISECONDS);
    jitter = new Random().nextInt(INIT_JITTER_BOUND);
    LOG.debug("Starting metric anomaly detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_metricAnomalyDetector,
                                           _metricAnomalyDetectionIntervalMs / 2 + jitter,
                                           _metricAnomalyDetectionIntervalMs,
                                           TimeUnit.MILLISECONDS);
    jitter = new Random().nextInt(INIT_JITTER_BOUND);
    LOG.debug("Starting disk failure detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_diskFailureDetector,
                                           _diskFailureDetectionIntervalMs / 2 + jitter,
                                           _diskFailureDetectionIntervalMs,
                                           TimeUnit.MILLISECONDS);
    _detectorScheduler.submit(new AnomalyHandlerTask());
  }

  /**
   * Shutdown the anomaly detector.
   * Note that if a fix is being started as shutdown is requested, shutdown will wait until the fix is initiated.
   */
  public void shutdown() {
    LOG.info("Shutting down anomaly detector.");
    synchronized (_shutdownLock) {
      _shutdown = true;
    }
    // SHUTDOWN_ANOMALY is a broker failure with detection time set to 0ms. Here we expect it is added to the front of the
    // priority queue and notify anomaly handler immediately.
    _anomalies.add(SHUTDOWN_ANOMALY);
    _detectorScheduler.shutdown();
    KafkaCruiseControlUtils.closeAdminClientWithTimeout(_adminClient);
    try {
      _detectorScheduler.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (!_detectorScheduler.isTerminated()) {
        LOG.warn("The sampling scheduler failed to shutdown in " + SCHEDULER_SHUTDOWN_TIMEOUT_MS + " ms.");
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for anomaly detector to shutdown.");
    }
    _brokerFailureDetector.shutdown();
    _anomalyLoggerExecutor.shutdownNow();
    LOG.info("Anomaly detector shutdown completed.");
  }

  /**
   * @return Anomaly detector state.
   */
  public synchronized AnomalyDetectorState anomalyDetectorState() {
    _anomalyDetectorState.refreshMetrics(_anomalyNotifier.selfHealingEnabledRatio(), _goalViolationDetector.balancednessScore());
    return _anomalyDetectorState;
  }

  /**
   * @return Number of anomaly fixes started by the anomaly detector for self healing.
   */
  long numSelfHealingStarted() {
    return _anomalyDetectorState.numSelfHealingStarted();
  }

  /**
   * See {@link AnomalyDetectorState#maybeClearOngoingAnomalyDetectionTimeMs}.
   */
  public void maybeClearOngoingAnomalyDetectionTimeMs() {
    _anomalyDetectorState.maybeClearOngoingAnomalyDetectionTimeMs();
  }

  /**
   * (1) Enable or disable self healing for the given anomaly type and (2) update the cached anomaly detector state.
   *
   * @param anomalyType Type of anomaly for which to enable or disable self healing.
   * @param isSelfHealingEnabled True if self healing is enabled, false otherwise.
   * @return The old value of self healing for the given anomaly type.
   */
  public boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    boolean oldSelfHealingEnabled = _anomalyNotifier.setSelfHealingFor(anomalyType, isSelfHealingEnabled);
    _anomalyDetectorState.setSelfHealingFor(anomalyType, isSelfHealingEnabled);

    return oldSelfHealingEnabled;
  }

  /**
   * @return Number of anomalies checked with delay.
   */
  public long numCheckedWithDelay() {
    return _numCheckedWithDelay;
  }

  /**
   * Update anomaly status once associated self-healing operation has finished.
   *
   * @param anomalyId Unique id of anomaly which triggered self-healing operation.
   */
  public void markSelfHealingFinished(String anomalyId) {
    _anomalyDetectorState.markSelfHealingFinished(anomalyId);
  }

  /**
   * A class that handles all the anomalies.
   */
  class AnomalyHandlerTask implements Runnable {
    @Override
    public void run() {
      LOG.info("Starting anomaly handler");
      while (true) {
        // In case handling the anomaly in progress fails, do some post processing.
        boolean postProcessAnomalyInProgress = false;
        _anomalyInProgress = null;
        try {
          _anomalyInProgress = _anomalies.take();
          LOG.trace("Processing anomaly {}.", _anomalyInProgress);
          if (_anomalyInProgress == SHUTDOWN_ANOMALY) {
            // Service has shutdown.
            _anomalyInProgress = null;
            break;
          }
          handleAnomalyInProgress();
        } catch (InterruptedException e) {
          LOG.debug("Received interrupted exception.", e);
          postProcessAnomalyInProgress = true;
        } catch (OptimizationFailureException ofe) {
          LOG.warn("Encountered optimization failure when trying to fix the anomaly {}.", _anomalyInProgress, ofe);
          // If self-healing failed due to an optimization failure, that indicates a hard goal violation; hence there is
          // no further processing anomaly detector can do without human intervention for the anomaly (i.e. other than
          // what has already been done in the {@link #handlePostFixAnomaly(boolean, boolean, String)}).
          postProcessAnomalyInProgress = false;
        } catch (IllegalStateException ise) {
          LOG.warn("Unexpected state prevents anomaly detector from handling the anomaly {}.", _anomalyInProgress, ise);
          // An illegal state may indicate a transient process blocking self-healing (e.g. an ongoing execution not
          // started by Cruise Control).
          postProcessAnomalyInProgress = false;
        } catch (Throwable t) {
          LOG.error("Uncaught exception in anomaly handler.", t);
          postProcessAnomalyInProgress = true;
        }
        if (postProcessAnomalyInProgress) {
          LOG.info("Post processing anomaly {}.", _anomalyInProgress);
          postProcessAnomalyInProgress(_brokerFailureDetectionBackoffMs);
        }
      }
      LOG.info("Anomaly handler exited.");
    }

    private void handleAnomalyInProgress() throws Exception {
      // Add anomaly detection to anomaly detector state.
      AnomalyType anomalyType = _anomalyInProgress.anomalyType();
      _anomalyDetectorState.addAnomalyDetection(anomalyType, _anomalyInProgress);

      // We schedule a delayed check if the executor is doing some work.
      ExecutorState.State executionState = _kafkaCruiseControl.executionState();
      if (executionState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
        LOG.info("Post processing anomaly {} because executor is in {} state.", _anomalyInProgress, executionState);
        postProcessAnomalyInProgress(_brokerFailureDetectionBackoffMs);
      } else {
        processAnomalyInProgress(anomalyType);
      }
    }

    /**
     * Processes the anomaly based on the notification result.
     *
     * @param anomalyType The type of the ongoing anomaly
     */
    private void processAnomalyInProgress(AnomalyType anomalyType) throws Exception {
      _anomalyDetectorState.markAnomalyRate(anomalyType);
      // Call the anomaly notifier to see if an action is requested.
      AnomalyNotificationResult notificationResult = notifyAnomalyInProgress(anomalyType);
      if (notificationResult != null) {
        _anomalyDetectorState.maybeSetOngoingAnomalyDetectionTimeMs();
        switch (notificationResult.action()) {
          case FIX:
            fixAnomalyInProgress(anomalyType);
            break;
          case CHECK:
            LOG.info("Post processing anomaly {} for {}.", _anomalyInProgress, AnomalyState.Status.CHECK_WITH_DELAY);
            postProcessAnomalyInProgress(notificationResult.delay());
            break;
          case IGNORE:
            _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, AnomalyState.Status.IGNORED);
            break;
          default:
            throw new IllegalStateException("Unrecognized anomaly notification result.");
        }
      }
    }

    /**
     * Call the {@link AnomalyNotifier} handler corresponding to the type of {@link #_anomalyInProgress} to get the
     * notification result.
     *
     * @param anomalyType The type of the {@link #_anomalyInProgress}.
     * @return The notification result corresponding to the {@link #_anomalyInProgress}.
     */
    private AnomalyNotificationResult notifyAnomalyInProgress(AnomalyType anomalyType) {
      // Call the anomaly notifier to see if a fix is desired.
      AnomalyNotificationResult notificationResult;
      switch ((KafkaAnomalyType) anomalyType) {
        case GOAL_VIOLATION:
          GoalViolations goalViolations = (GoalViolations) _anomalyInProgress;
          notificationResult = _anomalyNotifier.onGoalViolation(goalViolations);
          break;
        case BROKER_FAILURE:
          BrokerFailures brokerFailures = (BrokerFailures) _anomalyInProgress;
          notificationResult = _anomalyNotifier.onBrokerFailure(brokerFailures);
          break;
        case METRIC_ANOMALY:
          KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) _anomalyInProgress;
          notificationResult = _anomalyNotifier.onMetricAnomaly(metricAnomaly);
          break;
        case DISK_FAILURE:
          DiskFailures diskFailures = (DiskFailures) _anomalyInProgress;
          notificationResult = _anomalyNotifier.onDiskFailure(diskFailures);
          break;
        default:
          throw new IllegalStateException("Unrecognized anomaly type.");
      }
      LOG.debug("Received notification result {}", notificationResult);

      return notificationResult;
    }

    /**
     * Updates the state of the anomaly in progress and if the anomaly is a {@link KafkaAnomalyType#BROKER_FAILURE}, then it
     * schedules a broker failure detection after the given delay.
     *
     * @param delayMs The delay for broker failure detection.
     */
    private void postProcessAnomalyInProgress(long delayMs) {
      // Anomaly detector does delayed check for broker failures, otherwise it ignores the anomaly.
      if (_anomalyInProgress.anomalyType() == KafkaAnomalyType.BROKER_FAILURE) {
        synchronized (_shutdownLock) {
          if (_shutdown) {
            LOG.debug("Skip delayed checking anomaly {}, because anomaly detector is shutting down.", _anomalyInProgress);
          } else {
            LOG.debug("Scheduling broker failure detection with delay of {} ms", delayMs);
            _numCheckedWithDelay++;
            _detectorScheduler.schedule(() -> _brokerFailureDetector.detectBrokerFailures(false), delayMs, TimeUnit.MILLISECONDS);
            _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, AnomalyState.Status.CHECK_WITH_DELAY);
          }
        }
      } else {
        _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, AnomalyState.Status.IGNORED);
      }
    }

    /**
     * Check whether the anomaly in progress is ready for fix. An anomaly is ready if it (1) meets completeness
     * requirements and (2) load monitor is not in an unexpected state.
     *
     * @return True if ready for a fix, false otherwise.
     */
    private boolean isAnomalyInProgressReadyToFix(AnomalyType anomalyType) {
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _kafkaCruiseControl.getLoadMonitorTaskRunnerState();

      // Fixing anomalies is possible only when (1) the state is not in and unavailable state ( e.g. loading or
      // bootstrapping) and (2) the completeness requirements are met for all goals.
      if (!AnomalyUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
        LOG.info("Skipping {} fix because load monitor is in {} state.", anomalyType, loadMonitorTaskRunnerState);
        _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, AnomalyState.Status.LOAD_MONITOR_NOT_READY);
      } else {
        if (_kafkaCruiseControl.meetCompletenessRequirements(_selfHealingGoals)) {
          return true;
        } else {
          LOG.warn("Skipping {} fix because load completeness requirement is not met for goals.", anomalyType);
          _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, AnomalyState.Status.COMPLETENESS_NOT_READY);
        }
      }
      return false;
    }

    private void logSelfHealingOperation(String anomalyId, OptimizationFailureException ofe, String optimizationResult) {
      if (optimizationResult != null) {
        OPERATION_LOG.info("[{}] Self-healing started successfully:\n{}", anomalyId, optimizationResult);
      } else {
        OPERATION_LOG.warn("[{}] Self-healing failed to start:\n{}", anomalyId, ofe);
      }
    }

    private void fixAnomalyInProgress(AnomalyType anomalyType) throws Exception {
      synchronized (_shutdownLock) {
        if (_shutdown) {
          LOG.info("Skip fixing anomaly {}, because anomaly detector is shutting down.", _anomalyInProgress);
        } else {
          boolean isReadyToFix = isAnomalyInProgressReadyToFix(anomalyType);
          boolean fixStarted = false;
          String anomalyId = _anomalyInProgress.anomalyId();
          // Upon post-handling the anomaly, skip reporting broker failure if the failed brokers have not changed.
          boolean skipReportingIfNotUpdated = false;
          try {
            if (isReadyToFix) {
              LOG.info("Fixing anomaly {}", _anomalyInProgress);
              fixStarted = _anomalyInProgress.fix();
              String optimizationResult = _anomalyInProgress.optimizationResult(false);
              _anomalyLoggerExecutor.submit(() -> logSelfHealingOperation(anomalyId, null, optimizationResult));
            }
          } catch (OptimizationFailureException ofe) {
            _anomalyLoggerExecutor.submit(() -> logSelfHealingOperation(anomalyId, ofe, null));
            skipReportingIfNotUpdated = anomalyType == KafkaAnomalyType.BROKER_FAILURE;
            throw ofe;
          } finally {
            handlePostFixAnomaly(isReadyToFix, fixStarted, anomalyId, skipReportingIfNotUpdated);
          }
        }
      }
    }

    private void handlePostFixAnomaly(boolean isReadyToFix, boolean fixStarted, String anomalyId, boolean skipReportingIfNotUpdated) {
      if (isReadyToFix) {
        _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, fixStarted ? AnomalyState.Status.FIX_STARTED
                                                                             : AnomalyState.Status.FIX_FAILED_TO_START);
        if (fixStarted) {
          _anomalyDetectorState.incrementNumSelfHealingStarted();
          LOG.info("[{}] Self-healing started successfully.", anomalyId);
        } else {
          LOG.warn("[{}] Self-healing failed to start.", anomalyId);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Clearing {} anomalies and scheduling a broker failure detection in {}ms.", _anomalies.size(),
                  isReadyToFix ? 0L : _brokerFailureDetectionBackoffMs);
      }
      _anomalies.clear();
      // Explicitly detect broker failures after clearing the queue. This ensures that anomaly detector does not miss
      // broker failures upon (1) fixing another anomaly, or (2) having broker failures that are not yet ready for fix.
      // We don't need to worry about other anomaly types because they run periodically.
      // If there has not been any failed brokers at the time of detecting broker failures, this is a no-op. Otherwise,
      // the call will create a broker failure anomaly. Depending on the time of the first broker failure in that anomaly,
      // it will trigger either a delayed check or a fix.
      _detectorScheduler.schedule(() -> _brokerFailureDetector.detectBrokerFailures(skipReportingIfNotUpdated),
                                  isReadyToFix ? 0L : _brokerFailureDetectionBackoffMs, TimeUnit.MILLISECONDS);
    }
  }
}
