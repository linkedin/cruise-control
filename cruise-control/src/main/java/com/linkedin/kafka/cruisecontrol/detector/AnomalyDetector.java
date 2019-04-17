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
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyType;
import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.EXECUTOR;


/**
 * The anomaly detector class that helps detect and handle anomalies.
 */
public class AnomalyDetector {
  private static final String METRIC_REGISTRY_NAME = "AnomalyDetector";
  private static final int INIT_JITTER_BOUND = 10000;
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
  private static final Anomaly SHUTDOWN_ANOMALY = new BrokerFailures(null,
                                                                     Collections.emptyMap(),
                                                                     true,
                                                                     true,
                                                                     true);
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
  private final Meter _brokerFailureRate;
  private final Meter _goalViolationRate;
  private final Meter _metricAnomalyRate;
  private final Meter _diskFailureRate;
  private final LoadMonitor _loadMonitor;
  private final AnomalyDetectorState _anomalyDetectorState;

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
    _goalViolationDetector = new GoalViolationDetector(config, _loadMonitor, _anomalies, time, _kafkaCruiseControl);
    _brokerFailureDetector = new BrokerFailureDetector(config, _loadMonitor, _anomalies, time, _kafkaCruiseControl);
    _metricAnomalyDetector = new MetricAnomalyDetector(config, _loadMonitor, _anomalies, _kafkaCruiseControl);
    _diskFailureDetector = new DiskFailureDetector(config, _loadMonitor, _adminClient, _anomalies, time, _kafkaCruiseControl);
    _detectorScheduler =
        Executors.newScheduledThreadPool(5, new KafkaCruiseControlThreadFactory(METRIC_REGISTRY_NAME, false, LOG));
    _shutdown = false;
    _brokerFailureRate = dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "broker-failure-rate"));
    _goalViolationRate = dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "goal-violation-rate"));
    _metricAnomalyRate = dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "metric-anomaly-rate"));
    _diskFailureRate = dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "disk-failure-rate"));
    // Add anomaly detector state
    int numCachedRecentAnomalyStates = config.getInt(KafkaCruiseControlConfig.NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG);
    _anomalyDetectorState = new AnomalyDetectorState(_anomalyNotifier.selfHealingEnabled(), numCachedRecentAnomalyStates);
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
    _brokerFailureRate = new Meter();
    _goalViolationRate = new Meter();
    _metricAnomalyRate = new Meter();
    _diskFailureRate = new Meter();
    _loadMonitor = loadMonitor;
    // Add anomaly detector state
    _anomalyDetectorState = new AnomalyDetectorState(new HashMap<>(AnomalyType.cachedValues().size()), 10);
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
    jitter = new Random().nextInt(INIT_JITTER_BOUND);
    LOG.debug("Starting disk failure detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_diskFailureDetector,
                                          _anomalyDetectionIntervalMs / 2 + jitter,
                                           _anomalyDetectionIntervalMs,
                                           TimeUnit.MILLISECONDS);
  }

  /**
   * Shutdown the metric fetcher manager.
   */
  public void shutdown() {
    LOG.info("Shutting down anomaly detector.");
    _shutdown = true;
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
   * A class that handles all the anomalies.
   */
  class AnomalyHandlerTask implements Runnable {
    @Override
    public void run() {
      LOG.info("Starting anomaly handler");
      while (true) {
        Anomaly anomaly = null;
        boolean retryHandling = false;
        try {
          anomaly = _anomalies.take();
          LOG.trace("Received anomaly {}", anomaly);
          if (anomaly == SHUTDOWN_ANOMALY) {
            // Service has shutdown.
            break;
          }
          // Add anomaly detection to anomaly detector state.
          AnomalyType anomalyType = getAnomalyType(anomaly);
          _anomalyDetectorState.addAnomalyDetection(anomalyType, anomaly);

          // We schedule a delayed check if the executor is doing some work.
          ExecutorState.State executorState = _kafkaCruiseControl.state(
              new OperationProgress(), Collections.singleton(EXECUTOR)).executorState().state();
          if (executorState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
            LOG.debug("Schedule delayed check for anomaly {} because executor is in {} state", anomaly, executorState);
            checkWithDelay(anomaly, _anomalyDetectionIntervalMs);
          } else {
            AnomalyNotificationResult notificationResult = null;
            // Call the anomaly notifier to see if a fix is desired.
            switch (anomalyType) {
              case GOAL_VIOLATION:
                GoalViolations goalViolations = (GoalViolations) anomaly;
                notificationResult = _anomalyNotifier.onGoalViolation(goalViolations);
                _goalViolationRate.mark();
                break;
              case BROKER_FAILURE:
                BrokerFailures brokerFailures = (BrokerFailures) anomaly;
                notificationResult = _anomalyNotifier.onBrokerFailure(brokerFailures);
                _brokerFailureRate.mark();
                break;
              case METRIC_ANOMALY:
                KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) anomaly;
                notificationResult = _anomalyNotifier.onMetricAnomaly(metricAnomaly);
                _metricAnomalyRate.mark();
                break;
              case DISK_FAILURE:
                DiskFailures diskFailures = (DiskFailures) anomaly;
                notificationResult = _anomalyNotifier.onDiskFailure(diskFailures);
                _diskFailureRate.mark();
                break;
              default:
                throw new IllegalStateException("Unrecognized anomaly type.");
            }
            // Take the requested action if provided.
            LOG.debug("Received notification result {}", notificationResult);
            if (notificationResult != null) {
              switch (notificationResult.action()) {
                case FIX:
                  fixAnomaly(anomaly);
                  break;
                case CHECK:
                  checkWithDelay(anomaly, notificationResult.delay());
                  break;
                default:
                  // let it go.
                  _anomalyDetectorState.onAnomalyHandle(anomaly, AnomalyState.Status.IGNORED);
                  break;
              }
            }
          }
        } catch (InterruptedException e) {
          LOG.debug("Received interrupted exception.", e);
          retryHandling = true;
          // Let it go.
        } catch (KafkaCruiseControlException kcce) {
          LOG.warn("Anomaly handler received exception when try to fix the anomaly {}.", anomaly, kcce);
          retryHandling = true;
        } catch (Throwable t) {
          LOG.error("Uncaught exception in anomaly handler.", t);
          retryHandling = true;
        }
        if (retryHandling && anomaly != null) {
          checkWithDelay(anomaly, _anomalyDetectionIntervalMs);
        }
      }
      LOG.info("Anomaly handler exited.");
    }

    private void checkWithDelay(Anomaly anomaly, long delay) {
      // Anomaly detector does delayed check for broker failures, otherwise it ignores the anomaly.
      if (getAnomalyType(anomaly) == AnomalyType.BROKER_FAILURE) {
        LOG.debug("Scheduling broker failure detection with delay of {} ms", delay);
        _detectorScheduler.schedule(_brokerFailureDetector::detectBrokerFailures, delay, TimeUnit.MILLISECONDS);
        _anomalyDetectorState.onAnomalyHandle(anomaly, AnomalyState.Status.CHECK_WITH_DELAY);
      } else {
        _anomalyDetectorState.onAnomalyHandle(anomaly, AnomalyState.Status.IGNORED);
      }
    }

    /**
     * Check whether the given anomaly is ready for fix. An anomaly is ready if it (1) meets completeness requirements
     * and (2) load monitor is not in an unexpected state.
     *
     * @param anomaly The anomaly to check whether it is ready for a fix or not.
     * @return true if ready for a fix, false otherwise.
     */
    private boolean isReadyToFix(Anomaly anomaly) {
      String skipMsg = String.format("%s fix", getAnomalyType(anomaly));
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();

      // Fixing anomalies is possible only when (1) the state is not in and unavailable state ( e.g. loading or
      // bootstrapping) and (2) the completeness requirements are met for all goals.
      if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
        LOG.info("Skipping {} because load monitor is in {} state.", skipMsg, loadMonitorTaskRunnerState);
        _anomalyDetectorState.onAnomalyHandle(anomaly, AnomalyState.Status.LOAD_MONITOR_NOT_READY);
      } else {
        boolean meetCompletenessRequirements = _kafkaCruiseControl.meetCompletenessRequirements(Collections.emptyList());
        if (meetCompletenessRequirements) {
          return true;
        } else {
          LOG.warn("Skipping {} because load completeness requirement is not met for goals.", skipMsg);
          _anomalyDetectorState.onAnomalyHandle(anomaly, AnomalyState.Status.COMPLETENESS_NOT_READY);
        }
      }
      return false;
    }

    private void fixAnomaly(Anomaly anomaly) throws Exception {
      if (isReadyToFix(anomaly)) {
        LOG.info("Fixing anomaly {}", anomaly);
        boolean startedSuccessfully = false;
        try {
          startedSuccessfully = anomaly.fix();
        } finally {
          _anomalyDetectorState.onAnomalyHandle(anomaly, startedSuccessfully ? AnomalyState.Status.FIX_STARTED
                                                                             : AnomalyState.Status.FIX_FAILED_TO_START);
          LOG.info("Self-healing {}.", startedSuccessfully ? "started successfully" : "failed to start");
        }
      }

      _anomalies.clear();
      // We need to add the shutdown message in case the failure detector has shutdown.
      if (_shutdown) {
        _anomalies.addFirst(SHUTDOWN_ANOMALY);
      }
      // Explicitly detect broker failures after clear the queue.
      // We don't need to worry about the goal violation because it is run periodically.
      _brokerFailureDetector.detectBrokerFailures();
    }
  }
}
