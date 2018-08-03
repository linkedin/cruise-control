/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState.SubState.EXECUTOR;


/**
 * The anomaly detector class that helps detect and handle anomalies.
 */
public class AnomalyDetector {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
  private static final Anomaly SHUTDOWN_ANOMALY = new BrokerFailures(null, Collections.emptyMap(), true);
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
  private final Meter _brokerFailureRate;
  private final Meter _goalViolationRate;
  private final Meter _metricAnomalyRate;
  private final LoadMonitor _loadMonitor;

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
    _goalViolationDetector = new GoalViolationDetector(config, _loadMonitor, _anomalies, time, _kafkaCruiseControl);
    _brokerFailureDetector = new BrokerFailureDetector(config, _loadMonitor, _anomalies, time, _kafkaCruiseControl);
    _metricAnomalyDetector = new MetricAnomalyDetector(config, _loadMonitor, _anomalies, _kafkaCruiseControl);
    _detectorScheduler =
        Executors.newScheduledThreadPool(4, new KafkaCruiseControlThreadFactory("AnomalyDetector", false, LOG));
    _shutdown = false;
    _brokerFailureRate = dropwizardMetricRegistry.meter(MetricRegistry.name("AnomalyDetector", "broker-failure-rate"));
    _goalViolationRate = dropwizardMetricRegistry.meter(MetricRegistry.name("AnomalyDetector", "goal-violation-rate"));
    _metricAnomalyRate = dropwizardMetricRegistry.meter(MetricRegistry.name("AnomalyDetector", "metric-anomaly-rate"));
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
    _brokerFailureRate = new Meter();
    _goalViolationRate = new Meter();
    _metricAnomalyRate = new Meter();
    _loadMonitor = loadMonitor;
  }

  public void startDetection() {
    LOG.info("Starting anomaly detector.");
    _brokerFailureDetector.startDetection();
    int jitter = new Random().nextInt(10000);
    LOG.debug("Starting goal violation detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_goalViolationDetector,
                                           _anomalyDetectionIntervalMs / 2 + jitter,
                                           _anomalyDetectionIntervalMs,
                                           TimeUnit.MILLISECONDS);
    jitter = new Random().nextInt(10000);
    LOG.debug("Starting metric anomaly detector with delay of {} ms", jitter);
    _detectorScheduler.scheduleAtFixedRate(_metricAnomalyDetector,
                                           _anomalyDetectionIntervalMs / 2 + jitter,
                                           _anomalyDetectionIntervalMs,
                                           TimeUnit.MILLISECONDS);
    _detectorScheduler.submit(new AnomalyHandlerTask());
  }

  /**
   * Shutdown the metric fetcher manager.
   */
  public void shutdown() {
    LOG.info("Shutting down anomaly detector.");
    _shutdown = true;
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
    LOG.info("Anomaly detector shutdown completed.");
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
          // We schedule a delayed check if the executor is doing some work.
          ExecutorState.State executorState = _kafkaCruiseControl.state(
              new OperationProgress(), Collections.singleton(EXECUTOR)).executorState().state();
          if (executorState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
            LOG.debug("Schedule delayed check for anomaly {} because executor is in {} state", anomaly, executorState);
            checkWithDelay(anomaly, _anomalyDetectionIntervalMs);
          } else {
            AnomalyNotificationResult notificationResult = null;
            // Call the anomaly notifier to see if a fix is desired.
            if (anomaly instanceof BrokerFailures) {
              BrokerFailures brokerFailures = (BrokerFailures) anomaly;
              notificationResult = _anomalyNotifier.onBrokerFailure(brokerFailures);
              _brokerFailureRate.mark();
            } else if (anomaly instanceof GoalViolations) {
              GoalViolations goalViolations = (GoalViolations) anomaly;
              notificationResult = _anomalyNotifier.onGoalViolation(goalViolations);
              _goalViolationRate.mark();
            } else if (anomaly instanceof KafkaMetricAnomaly) {
              KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) anomaly;
              notificationResult = _anomalyNotifier.onMetricAnomaly(metricAnomaly);
              _metricAnomalyRate.mark();
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
      // We only do delayed check for broker failures.
      if (anomaly instanceof BrokerFailures) {
        LOG.debug("Scheduling broker failure detection with delay of {} ms", delay);
        _detectorScheduler.schedule(_brokerFailureDetector::detectBrokerFailures, delay, TimeUnit.MILLISECONDS);
      }
    }

    /**
     * Check whether the given anomaly is fixable. An anomaly is fixable if it (1) meets completeness requirements
     * and (2) load monitor is not in an unexpected state.
     *
     * @param anomaly The anomaly to check whether fixable or not.
     * @return true if fixable, false otherwise.
     */
    private boolean isFixable(Anomaly anomaly) {
      String skipMsg = (anomaly instanceof GoalViolations) ? "goal violation fix" : "broker failure fix";
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();

      // Fixing anomalies is possible only when (1) the state is not in and unavailable state ( e.g. loading or
      // bootstrapping) and (2) the completeness requirements are met for all goals.
      if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
        LOG.info("Skipping {} because load monitor is in {} state.", skipMsg, loadMonitorTaskRunnerState);
      } else {
        boolean meetCompletenessRequirements = _kafkaCruiseControl.meetCompletenessRequirements(Collections.emptyList());
        if (meetCompletenessRequirements) {
          return true;
        } else {
          LOG.debug("Skipping {} because load completeness requirement is not met for goals.", skipMsg);
        }
      }
      return false;
    }

    private void fixAnomaly(Anomaly anomaly) throws Exception {
      if (isFixable(anomaly)) {
        LOG.info("Fixing anomaly {}", anomaly);
        anomaly.fix();
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
