/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.OPERATION_LOGGER;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyType;
import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.EXECUTOR;


/**
 * The anomaly detector class that helps detect and handle anomalies.
 */
public class AnomalyDetector {
  private static final String METRIC_REGISTRY_NAME = "AnomalyDetector";
  private static final int INIT_JITTER_BOUND = 10000;
  private static final int NUM_ANOMALY_DETECTION_THREADS = 5;
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
  private static final Logger OPERATION_LOG = LoggerFactory.getLogger(OPERATION_LOGGER);
  private static final Anomaly SHUTDOWN_ANOMALY = new BrokerFailures(null,
                                                                     Collections.emptyMap(),
                                                                     true,
                                                                     true,
                                                                     true,
                                                                     Collections.emptyList());
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final AnomalyNotifier _anomalyNotifier;
  private final AdminClient _adminClient;
  // Detectors
  private final GoalViolationDetector _goalViolationDetector;
  private final BrokerFailureDetector _brokerFailureDetector;
  private final MetricAnomalyDetector _metricAnomalyDetector;
  private final DiskFailureDetector _diskFailureDetector;
  private final ScheduledExecutorService _detectorScheduler;
  private final long _anomalyDetectionIntervalMs;
  private final LinkedBlockingDeque<Anomaly> _anomalies;
  private volatile boolean _shutdown;
  private final Map<AnomalyType, Meter> _anomalyRateByType;
  private final LoadMonitor _loadMonitor;
  private final AnomalyDetectorState _anomalyDetectorState;
  // TODO: Make this configurable.
  private final List<String> _selfHealingGoals;
  private final ExecutorService _anomalyLoggerExecutor;
  private volatile Anomaly _anomalyInProgress;
  private final Object _shutdownLock;

  public AnomalyDetector(KafkaCruiseControlConfig config,
                         LoadMonitor loadMonitor,
                         KafkaCruiseControl kafkaCruiseControl,
                         Time time,
                         MetricRegistry dropwizardMetricRegistry) {
    _anomalies = new LinkedBlockingDeque<>();
    _adminClient = KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config));
    _anomalyDetectionIntervalMs = config.getLong(KafkaCruiseControlConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    _anomalyNotifier = config.getConfiguredInstance(KafkaCruiseControlConfig.ANOMALY_NOTIFIER_CLASS_CONFIG,
                                                    AnomalyNotifier.class);
    _loadMonitor = loadMonitor;
    _kafkaCruiseControl = kafkaCruiseControl;
    _selfHealingGoals = Collections.emptyList();
    _kafkaCruiseControl.sanityCheckHardGoalPresence(_selfHealingGoals, false);
    _goalViolationDetector = new GoalViolationDetector(config, _loadMonitor, _anomalies, time, _kafkaCruiseControl, _selfHealingGoals);
    _brokerFailureDetector = new BrokerFailureDetector(config, _loadMonitor, _anomalies, time, _kafkaCruiseControl, _selfHealingGoals);
    _metricAnomalyDetector = new MetricAnomalyDetector(config, _loadMonitor, _anomalies, _kafkaCruiseControl);
    _diskFailureDetector = new DiskFailureDetector(config, _loadMonitor, _adminClient, _anomalies, time, _kafkaCruiseControl, _selfHealingGoals);
    _detectorScheduler = Executors.newScheduledThreadPool(NUM_ANOMALY_DETECTION_THREADS,
                                                          new KafkaCruiseControlThreadFactory(METRIC_REGISTRY_NAME, false, LOG));
    _shutdown = false;
    _anomalyRateByType = new HashMap<>(AnomalyType.cachedValues().size());
    _anomalyRateByType.put(AnomalyType.BROKER_FAILURE,
                           dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "broker-failure-rate")));
    _anomalyRateByType.put(AnomalyType.GOAL_VIOLATION,
                           dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "goal-violation-rate")));
    _anomalyRateByType.put(AnomalyType.METRIC_ANOMALY,
                           dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "metric-anomaly-rate")));
    _anomalyRateByType.put(AnomalyType.DISK_FAILURE,
                           dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "disk-failure-rate")));
    // Add anomaly detector state
    int numCachedRecentAnomalyStates = config.getInt(KafkaCruiseControlConfig.NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG);
    _anomalyDetectorState = new AnomalyDetectorState(_anomalyNotifier.selfHealingEnabled(), numCachedRecentAnomalyStates);
    _anomalyLoggerExecutor =
        Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("AnomalyLogger", true, null));
    _anomalyInProgress = null;
    _shutdownLock = new Object();
  }

  /**
   * Package private constructor for unit test.
   */
  AnomalyDetector(LinkedBlockingDeque<Anomaly> anomalies,
                  AdminClient adminClient,
                  long anomalyDetectionIntervalMs,
                  KafkaCruiseControl kafkaCruiseControl,
                  AnomalyNotifier anomalyNotifier,
                  GoalViolationDetector goalViolationDetector,
                  BrokerFailureDetector brokerFailureDetector,
                  MetricAnomalyDetector metricAnomalyDetector,
                  DiskFailureDetector diskFailureDetector,
                  ScheduledExecutorService detectorScheduler,
                  LoadMonitor loadMonitor) {
    _anomalies = anomalies;
    _adminClient = adminClient;
    _anomalyDetectionIntervalMs = anomalyDetectionIntervalMs;
    _anomalyNotifier = anomalyNotifier;
    _goalViolationDetector = goalViolationDetector;
    _brokerFailureDetector = brokerFailureDetector;
    _metricAnomalyDetector = metricAnomalyDetector;
    _diskFailureDetector = diskFailureDetector;
    _kafkaCruiseControl = kafkaCruiseControl;
    _detectorScheduler = detectorScheduler;
    _shutdown = false;
    _anomalyRateByType = new HashMap<>(AnomalyType.cachedValues().size());
    AnomalyType.cachedValues().forEach(anomalyType -> _anomalyRateByType.put(anomalyType, new Meter()));
    _loadMonitor = loadMonitor;
    // Add anomaly detector state
    _anomalyDetectorState = new AnomalyDetectorState(new HashMap<>(AnomalyType.cachedValues().size()), 10);
    _selfHealingGoals = Collections.emptyList();
    _anomalyLoggerExecutor =
        Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("AnomalyLogger", true, null));
    _anomalyInProgress = null;
    _shutdownLock = new Object();
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
    jitter = new Random().nextInt(INIT_JITTER_BOUND);
    LOG.debug("Starting disk failure detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_diskFailureDetector,
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
    KafkaCruiseControlUtils.closeAdminClientWithTimeout(_adminClient);

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

  public AnomalyDetectorState anomalyDetectorState() {
    return _anomalyDetectorState;
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
   * Package private for unit tests.
   */
  boolean isAnomalyInProgress() {
    return _anomalyInProgress != null;
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
          // Add anomaly detection to anomaly detector state.
          AnomalyType anomalyType = getAnomalyType(_anomalyInProgress);
          _anomalyDetectorState.addAnomalyDetection(anomalyType, _anomalyInProgress);

          // We schedule a delayed check if the executor is doing some work.
          ExecutorState.State executorState = _kafkaCruiseControl.state(
              new OperationProgress(), Collections.singleton(EXECUTOR)).executorState().state();
          if (executorState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
            LOG.debug("Schedule delayed check for anomaly {} because executor is in {} state", _anomalyInProgress, executorState);
            checkWithDelay(_anomalyDetectionIntervalMs);
          } else {
            AnomalyNotificationResult notificationResult = null;
            _anomalyRateByType.get(anomalyType).mark();
            // Call the anomaly notifier to see if a fix is desired.
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
              case DISK_FAILURE:
                DiskFailures diskFailures = (DiskFailures) _anomalyInProgress;
                notificationResult = _anomalyNotifier.onDiskFailure(diskFailures);
                break;
              default:
                throw new IllegalStateException("Unrecognized anomaly type.");
            }
            // Take the requested action if provided.
            LOG.debug("Received notification result {}", notificationResult);
            if (notificationResult != null) {
              switch (notificationResult.action()) {
                case FIX:
                  fixAnomalyInProgress();
                  break;
                case CHECK:
                  checkWithDelay(notificationResult.delay());
                  break;
                default:
                  // let it go.
                  _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, AnomalyState.Status.IGNORED);
                  break;
              }
            }
          }
        } catch (InterruptedException e) {
          LOG.debug("Received interrupted exception.", e);
          retryHandling = true;
          // Let it go.
        } catch (KafkaCruiseControlException kcce) {
          LOG.warn("Anomaly handler received exception when try to fix the anomaly {}.", _anomalyInProgress, kcce);
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

    private void checkWithDelay(long delay) {
      // Anomaly detector does delayed check for broker failures, otherwise it ignores the anomaly.
      if (getAnomalyType(_anomalyInProgress) == AnomalyType.BROKER_FAILURE) {
        synchronized (_shutdownLock) {
          if (_shutdown) {
            LOG.debug("Skip delayed checking anomaly {}, because anomaly detector is shutting down.", _anomalyInProgress);
          } else {
            LOG.debug("Scheduling broker failure detection with delay of {} ms", delay);
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
    private boolean isAnomalyInProgressReadyToFix() {
      String skipMsg = String.format("%s fix", getAnomalyType(_anomalyInProgress));
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();

      // Fixing anomalies is possible only when (1) the state is not in and unavailable state ( e.g. loading or
      // bootstrapping) and (2) the completeness requirements are met for all goals.
      if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
        LOG.info("Skipping {} because load monitor is in {} state.", skipMsg, loadMonitorTaskRunnerState);
        _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, AnomalyState.Status.LOAD_MONITOR_NOT_READY);
      } else {
        if (_kafkaCruiseControl.meetCompletenessRequirements(_selfHealingGoals)) {
          return true;
        } else {
          LOG.warn("Skipping {} because load completeness requirement is not met for goals.", skipMsg);
          _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, AnomalyState.Status.COMPLETENESS_NOT_READY);
        }
      }
      return false;
    }

    private String optimizationResult() {
      switch (getAnomalyType(_anomalyInProgress)) {
        case GOAL_VIOLATION:
        case BROKER_FAILURE:
        case DISK_FAILURE:
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

    private void fixAnomalyInProgress() throws Exception {
      synchronized (_shutdownLock) {
        if (_shutdown) {
          LOG.info("Skip fixing anomaly {}, because anomaly detector is shutting down.", _anomalyInProgress);
        } else {
          boolean isReadyToFix = isAnomalyInProgressReadyToFix();
          if (isReadyToFix) {
            LOG.info("Fixing anomaly {}", _anomalyInProgress);
            boolean startedSuccessfully = false;
            String anomalyId = _anomalyInProgress.anomalyId();
            try {
              startedSuccessfully = _anomalyInProgress.fix();
              String optimizationResult = optimizationResult();
              _anomalyLoggerExecutor.submit(() -> logSelfHealingOperation(anomalyId, null, optimizationResult));
            } catch (OptimizationFailureException ofe) {
              _anomalyLoggerExecutor.submit(() -> logSelfHealingOperation(anomalyId, ofe, null));
              throw ofe;
            } finally {
              _anomalyDetectorState.onAnomalyHandle(_anomalyInProgress, startedSuccessfully ? AnomalyState.Status.FIX_STARTED
                                                                                            : AnomalyState.Status.FIX_FAILED_TO_START);
              if (startedSuccessfully) {
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
      _detectorScheduler.schedule(_brokerFailureDetector::detectBrokerFailures,
                                  isReadyToFix ? 0L : _anomalyDetectionIntervalMs, TimeUnit.MILLISECONDS);
    }
  }
}
