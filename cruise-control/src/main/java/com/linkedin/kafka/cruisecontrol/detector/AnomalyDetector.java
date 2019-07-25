/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.OPERATION_LOGGER;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyType;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.SHUTDOWN_ANOMALY;


/**
 * The anomaly detector class that helps detect and handle anomalies.
 */
public class AnomalyDetector {
  static final String METRIC_REGISTRY_NAME = "AnomalyDetector";
  private static final int INIT_JITTER_BOUND = 10000;
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
  private static final Logger OPERATION_LOG = LoggerFactory.getLogger(OPERATION_LOGGER);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final AnomalyNotifier _anomalyNotifier;
  // Detectors
  private final GoalViolationDetector _goalViolationDetector;
  private final BrokerFailureDetector _brokerFailureDetector;
  private final MetricAnomalyDetector _metricAnomalyDetector;
  private final ScheduledExecutorService _detectorScheduler;
  private final long _anomalyDetectionIntervalMs;
  private final LinkedBlockingDeque<Anomaly> _anomalies;
  private volatile boolean _shutdown;
  private final LoadMonitor _loadMonitor;
  private final AnomalyDetectorState _anomalyDetectorState;
  private final List<String> _selfHealingGoals;
  private final ExecutorService _anomalyLoggerExecutor;
  private volatile Anomaly _anomalyInProgress;
  private volatile long _numCheckedWithDelay;
  private final Object _shutdownLock;

  public AnomalyDetector(KafkaCruiseControlConfig config,
                         LoadMonitor loadMonitor,
                         KafkaCruiseControl kafkaCruiseControl,
                         Time time,
                         MetricRegistry dropwizardMetricRegistry) {
    _anomalies = new LinkedBlockingDeque<>();
    _anomalyDetectionIntervalMs = config.getLong(KafkaCruiseControlConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    _anomalyNotifier = config.getConfiguredInstance(KafkaCruiseControlConfig.ANOMALY_NOTIFIER_CLASS_CONFIG,
                                                    AnomalyNotifier.class);
    _loadMonitor = loadMonitor;
    _kafkaCruiseControl = kafkaCruiseControl;
    _selfHealingGoals = getSelfHealingGoalNames(config);
    _kafkaCruiseControl.sanityCheckHardGoalPresence(_selfHealingGoals, false);
    _goalViolationDetector = new GoalViolationDetector(config, _loadMonitor, _anomalies, time, _kafkaCruiseControl, _selfHealingGoals);
    _brokerFailureDetector = new BrokerFailureDetector(config, _loadMonitor, _anomalies, time, _kafkaCruiseControl, _selfHealingGoals);
    _metricAnomalyDetector = new MetricAnomalyDetector(config, _loadMonitor, _anomalies, _kafkaCruiseControl);
    _detectorScheduler =
        Executors.newScheduledThreadPool(4, new KafkaCruiseControlThreadFactory(METRIC_REGISTRY_NAME, false, LOG));
    _shutdown = false;
    // Add anomaly detector state
    int numCachedRecentAnomalyStates = config.getInt(KafkaCruiseControlConfig.NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG);
    _anomalyLoggerExecutor =
        Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("AnomalyLogger", true, null));
    _anomalyInProgress = null;
    _numCheckedWithDelay = 0L;
    _shutdownLock = new Object();
    _anomalyDetectorState = new AnomalyDetectorState(time,
                                                     _anomalyNotifier.selfHealingEnabled(),
                                                     numCachedRecentAnomalyStates,
                                                     dropwizardMetricRegistry);
  }

  /**
   * Package private constructor for unit test.
   */
  AnomalyDetector(LinkedBlockingDeque<Anomaly> anomalies,
                  long anomalyDetectionIntervalMs,
                  KafkaCruiseControl kafkaCruiseControl,
                  AnomalyNotifier anomalyNotifier,
                  GoalViolationDetector goalViolationDetector,
                  BrokerFailureDetector brokerFailureDetector,
                  MetricAnomalyDetector metricAnomalyDetector,
                  ScheduledExecutorService detectorScheduler,
                  LoadMonitor loadMonitor) {
    _anomalies = anomalies;
    _anomalyDetectionIntervalMs = anomalyDetectionIntervalMs;
    _anomalyNotifier = anomalyNotifier;
    _goalViolationDetector = goalViolationDetector;
    _brokerFailureDetector = brokerFailureDetector;
    _metricAnomalyDetector = metricAnomalyDetector;
    _kafkaCruiseControl = kafkaCruiseControl;
    _detectorScheduler = detectorScheduler;
    _shutdown = false;
    _loadMonitor = loadMonitor;
    _selfHealingGoals = Collections.emptyList();
    _anomalyLoggerExecutor =
        Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("AnomalyLogger", true, null));
    _anomalyInProgress = null;
    _numCheckedWithDelay = 0L;
    _shutdownLock = new Object();
    // Add anomaly detector state
    _anomalyDetectorState = new AnomalyDetectorState(new SystemTime(), new HashMap<>(AnomalyType.cachedValues().size()), 10, null);
  }

  public void startDetection() {
    LOG.info("Starting anomaly detector.");
    _brokerFailureDetector.startDetection();
    int jitter = new Random().nextInt(INIT_JITTER_BOUND);
    LOG.debug("Starting goal violation detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_goalViolationDetector,
                                           _anomalyDetectionIntervalMs / 2 + jitter,
                                           _anomalyDetectionIntervalMs,
                                           TimeUnit.MILLISECONDS);
    jitter = new Random().nextInt(INIT_JITTER_BOUND);
    LOG.debug("Starting metric anomaly detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_metricAnomalyDetector,
                                           _anomalyDetectionIntervalMs / 2 + jitter,
                                           _anomalyDetectionIntervalMs,
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
    _anomalies.addFirst(SHUTDOWN_ANOMALY);
    _detectorScheduler.shutdown();

    try {
      _detectorScheduler.awaitTermination(_anomalyDetectionIntervalMs, TimeUnit.MILLISECONDS);
      if (!_detectorScheduler.isTerminated()) {
        LOG.warn("The sampling scheduler failed to shutdown in " + _anomalyDetectionIntervalMs + " ms.");
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for anomaly detector to shutdown.");
    }
    _brokerFailureDetector.shutdown();
    _anomalyLoggerExecutor.shutdownNow();
    LOG.info("Anomaly detector shutdown completed.");
  }

  public synchronized AnomalyDetectorState anomalyDetectorState() {
    _anomalyDetectorState.refreshMetrics(_anomalyNotifier.selfHealingEnabledRatio());
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
        boolean retryHandling = false;
        _anomalyInProgress = null;
        try {
          _anomalyInProgress = _anomalies.takeFirst();
          LOG.trace("Processing anomaly {}.", _anomalyInProgress);
          if (_anomalyInProgress == SHUTDOWN_ANOMALY) {
            // Service has shutdown.
            _anomalyInProgress = null;
            break;
          }
          handleAnomalyInProgress();
        } catch (InterruptedException e) {
          LOG.debug("Received interrupted exception.", e);
          retryHandling = true;
        } catch (KafkaCruiseControlException kcce) {
          LOG.warn("Anomaly handler received exception when trying to fix the anomaly {}.", _anomalyInProgress, kcce);
          retryHandling = true;
        } catch (Throwable t) {
          LOG.error("Uncaught exception in anomaly handler.", t);
          retryHandling = true;
        }
        if (retryHandling && _anomalyInProgress != null) {
          checkWithDelay(_anomalyDetectionIntervalMs);
        }
      }
      LOG.info("Anomaly handler exited.");
    }

    private void handleAnomalyInProgress() throws Exception {
      // Add anomaly detection to anomaly detector state.
      AnomalyType anomalyType = getAnomalyType(_anomalyInProgress);
      _anomalyDetectorState.addAnomalyDetection(anomalyType, _anomalyInProgress);

      // We schedule a delayed check if the executor is doing some work.
      ExecutorState.State executionState = _kafkaCruiseControl.executionState();
      if (executionState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
        LOG.debug("Schedule delayed check for anomaly {} because executor is in {} state", _anomalyInProgress, executionState);
        checkWithDelay(_anomalyDetectionIntervalMs);
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
            checkWithDelay(notificationResult.delay());
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
      AnomalyNotificationResult notificationResult = null;
      switch (anomalyType) {
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
        default:
          throw new IllegalStateException("Unrecognized anomaly type.");
      }
      LOG.debug("Received notification result {}", notificationResult);

      return notificationResult;
    }

    private void checkWithDelay(long delay) {
      // Anomaly detector does delayed check for broker failures, otherwise it ignores the anomaly.
      if (getAnomalyType(_anomalyInProgress) == AnomalyType.BROKER_FAILURE) {
        synchronized (_shutdownLock) {
          if (_shutdown) {
            LOG.debug("Skip delayed checking anomaly {}, because anomaly detector is shutting down.", _anomalyInProgress);
          } else {
            LOG.debug("Scheduling broker failure detection with delay of {} ms", delay);
            _numCheckedWithDelay++;
            _detectorScheduler.schedule(_brokerFailureDetector::detectBrokerFailures, delay, TimeUnit.MILLISECONDS);
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
     * @return true if ready for a fix, false otherwise.
     */
    private boolean isAnomalyInProgressReadyToFix(AnomalyType anomalyType) {
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();

      // Fixing anomalies is possible only when (1) the state is not in and unavailable state ( e.g. loading or
      // bootstrapping) and (2) the completeness requirements are met for all goals.
      if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
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

    private String optimizationResult(AnomalyType anomalyType) {
      switch (anomalyType) {
        case GOAL_VIOLATION:
        case BROKER_FAILURE:
          return ((KafkaAnomaly) _anomalyInProgress).optimizationResult(false);
        case METRIC_ANOMALY:
          return ((KafkaMetricAnomaly) _anomalyInProgress).optimizationResult(false);
        default:
          throw new IllegalStateException("Unrecognized anomaly type.");
      }
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
          if (isReadyToFix) {
            LOG.info("Fixing anomaly {}", _anomalyInProgress);
            boolean startedSuccessfully = false;
            String anomalyId = _anomalyInProgress.anomalyId();
            try {
              startedSuccessfully = _anomalyInProgress.fix();
              String optimizationResult = optimizationResult(anomalyType);
              _anomalyLoggerExecutor.submit(() -> logSelfHealingOperation(anomalyId, null, optimizationResult));
            } catch (OptimizationFailureException ofe) {
              _anomalyLoggerExecutor.submit(() -> logSelfHealingOperation(anomalyId, ofe, null));
              throw ofe;
            } finally {
              _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, startedSuccessfully ? AnomalyState.Status.FIX_STARTED
                                                                                            : AnomalyState.Status.FIX_FAILED_TO_START);
              if (startedSuccessfully) {
                _anomalyDetectorState.incrementNumSelfHealingStarted();
                LOG.info("[{}] Self-healing started successfully.", anomalyId);
              } else {
                LOG.warn("[{}] Self-healing failed to start.", anomalyId);
              }
            }
          }
          handlePostFixAnomaly(isReadyToFix);
        }
      }
    }

    private void handlePostFixAnomaly(boolean isReadyToFix) {
      _anomalies.clear();
      // Explicitly detect broker failures after clearing the queue. This ensures that anomaly detector does not miss
      // broker failures upon (1) fixing another anomaly, or (2) having broker failures that are not yet ready for fix.
      // We don't need to worry about other anomaly types because they run periodically.
      // If there has not been any failed brokers at the time of detecting broker failures, this is a no-op. Otherwise,
      // the call will create a broker failure anomaly. Depending on the time of the first broker failure in that anomaly,
      // it will trigger either a delayed check or a fix.
      _detectorScheduler.schedule(_brokerFailureDetector::detectBrokerFailures,
                                  isReadyToFix ? 0L : _anomalyDetectionIntervalMs, TimeUnit.MILLISECONDS);
    }
  }
}
