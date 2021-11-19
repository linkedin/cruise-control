/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


/**
 * This class will be scheduled to run periodically to check if the given goals are violated or not. An alert will be
 * triggered if one of the goals is not met.
 */
public class GoalViolationDetector extends BaseGoalViolationDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolationDetector.class);

  public GoalViolationDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
    super(anomalies, kafkaCruiseControl, dropwizardMetricRegistry);
  }

  @Override
  public void run() {
    if (getGoalViolationDetectionStatus() != AnomalyDetectionStatus.READY) {
      return;
    }

    AutoCloseable clusterModelSemaphore = null;
    try {
      Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl,
              ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
      GoalViolations goalViolations = _kafkaCruiseControl.config().getConfiguredInstance(AnomalyDetectorConfig.GOAL_VIOLATIONS_CLASS_CONFIG,
              GoalViolations.class,
              parameterConfigOverrides);

      boolean newModelNeeded = true;
      ClusterModel clusterModel = null;

      // Retrieve excluded brokers for leadership and replica move.
      ExecutorState executorState = null;
      if (_excludeRecentlyDemotedBrokers || _excludeRecentlyRemovedBrokers) {
        executorState = _kafkaCruiseControl.executorState();
      }

      Set<Integer> excludedBrokersForLeadership = _excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
              : Collections.emptySet();

      Set<Integer> excludedBrokersForReplicaMove = _excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
              : Collections.emptySet();

      ProvisionResponse provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
      boolean checkPartitionsWithRFGreaterThanNumRacks = true;
      final Timer.Context ctx = _goalViolationDetectionTimer.time();
      try {
        for (Goal goal : _detectionGoals) {
          if (_kafkaCruiseControl.loadMonitor().meetCompletenessRequirements(goal.clusterModelCompletenessRequirements())) {
            LOG.debug("Detecting if {} is violated.", goal.name());
            // Because the model generation could be slow, We only get new cluster model if needed.
            if (newModelNeeded) {
              if (clusterModelSemaphore != null) {
                clusterModelSemaphore.close();
              }
              clusterModelSemaphore = _kafkaCruiseControl.acquireForModelGeneration(new OperationProgress());
              // Make cluster model null before generating a new cluster model so the current one can be GCed.
              clusterModel = null;
              clusterModel = _kafkaCruiseControl.clusterModel(goal.clusterModelCompletenessRequirements(),
                      _allowCapacityEstimation,
                      new OperationProgress());

              // If the clusterModel contains dead brokers or disks, goal violation detector will ignore any goal violations.
              // Detection and fix for dead brokers/disks is the responsibility of broker/disk failure detector.
              if (skipDueToOfflineReplicas(clusterModel)) {
                return;
              }
              _lastCheckedModelGeneration = clusterModel.generation();
            }
            newModelNeeded = optimizeForGoal(clusterModel, goal, goalViolations, excludedBrokersForLeadership, excludedBrokersForReplicaMove,
                    checkPartitionsWithRFGreaterThanNumRacks);
            // CC will check for partitions with RF greater than number of eligible racks just once, because regardless of the goal, the cluster
            // will have the same (1) maximum replication factor and (2) rack count containing brokers that are eligible to host replicas.
            checkPartitionsWithRFGreaterThanNumRacks = false;
          } else {
            LOG.warn("Skipping goal violation detection for {} because load completeness requirement is not met.", goal);
          }
          provisionResponse.aggregate(goal.provisionResponse());
        }
      } finally {
        ctx.stop();
      }
      _provisionResponse = provisionResponse;
      if (_isProvisionerEnabled) {
        // Rightsize the cluster (if needed)
        ProvisionerState provisionerState = _provisioner.rightsize(_provisionResponse.recommendationByRecommender(), new RightsizeOptions());
        if (provisionerState != null) {
          LOG.info("Provisioner state: {}.", provisionerState);
          _automatedRightsizingMeter.mark();
        }
      }
      Map<Boolean, List<String>> violatedGoalsByFixability = goalViolations.violatedGoalsByFixability();
      if (!violatedGoalsByFixability.isEmpty()) {
        goalViolations.setProvisionResponse(_provisionResponse);
        _anomalies.add(goalViolations);
      }
      refreshBalancednessScore(violatedGoalsByFixability);
    } catch (NotEnoughValidWindowsException nevwe) {
      LOG.debug("Skipping goal violation detection because there are not enough valid windows.", nevwe);
    } catch (KafkaCruiseControlException kcce) {
      LOG.warn("Goal violation detector received exception", kcce);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      if (clusterModelSemaphore != null) {
        try {
          clusterModelSemaphore.close();
        } catch (Exception e) {
          LOG.error("Received exception when closing auto closable semaphore", e);
        }
      }
      LOG.debug("Goal violation detection finished.");
    }
  }

}
