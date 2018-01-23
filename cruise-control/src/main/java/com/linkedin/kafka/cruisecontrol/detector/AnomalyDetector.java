/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The anomaly detector class that helps detect and handle anomalies.
 */
public class AnomalyDetector {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
  private static final Anomaly SHUTDOWN_ANOMALY = new BrokerFailures(Collections.emptyMap());
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final AnomalyNotifier _anomalyNotifier;
  // Detectors
  private final GoalViolationDetector _goalViolationDetector;
  private final BrokerFailureDetector _brokerFailureDetector;
  private final ScheduledExecutorService _detectorScheduler;
  private final long _anomalyDetectionIntervalMs;
  private final LinkedBlockingDeque<Anomaly> _anomalies;
  private volatile boolean _shutdown;
  private final Meter _brokerFailureRate;
  private final Meter _goalViolationRate;

  public AnomalyDetector(KafkaCruiseControlConfig config,
                         LoadMonitor loadMonitor,
                         KafkaCruiseControl kafkaCruiseControl,
                         Time time,
                         MetricRegistry dropwizardMetricRegistry) {
    _anomalies = new LinkedBlockingDeque<>();
    _anomalyDetectionIntervalMs = config.getLong(KafkaCruiseControlConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    _anomalyNotifier = config.getConfiguredInstance(KafkaCruiseControlConfig.ANOMALY_NOTIFIER_CLASS_CONFIG,
                                                    AnomalyNotifier.class);
    _goalViolationDetector = new GoalViolationDetector(config, loadMonitor, _anomalies, time);
    _brokerFailureDetector = new BrokerFailureDetector(config, loadMonitor, _anomalies, time);
    _kafkaCruiseControl = kafkaCruiseControl;
    _detectorScheduler =
        Executors.newScheduledThreadPool(3, new KafkaCruiseControlThreadFactory("AnomalyDetector", false, LOG));
    _shutdown = false;
    _brokerFailureRate = dropwizardMetricRegistry.meter(MetricRegistry.name("AnomalyDetector", "broker-failure-rate"));
    _goalViolationRate = dropwizardMetricRegistry.meter(MetricRegistry.name("AnomalyDetector", "goal-violation-rate"));

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
                  ScheduledExecutorService detectorScheduler) {
    _anomalies = anomalies;
    _anomalyDetectionIntervalMs = anomalyDetectionIntervalMs;
    _anomalyNotifier = anomalyNotifier;
    _goalViolationDetector = goalViolationDetector;
    _brokerFailureDetector = brokerFailureDetector;
    _kafkaCruiseControl = kafkaCruiseControl;
    _detectorScheduler = detectorScheduler;
    _shutdown = false;
    _brokerFailureRate = new Meter();
    _goalViolationRate = new Meter();
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
          ExecutorState.State executorState = _kafkaCruiseControl.state(new OperationProgress()).executorState().state();
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

    private void fixAnomaly(Anomaly anomaly) throws KafkaCruiseControlException {
      LOG.info("Fixing anomaly {}", anomaly);
      anomaly.fix(_kafkaCruiseControl);
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
