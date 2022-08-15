/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ANOMALY_DETECTOR_SENSOR;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


/**
 * This class will be scheduled to run periodically to check if the given goals are violated or not. An alert will be
 * triggered if one of the goals is not met.
 */
public class IntraBrokerGoalViolationDetector extends BaseGoalViolationDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerGoalViolationDetector.class);
  private final List<Goal> _detectionGoals;
  private final Timer _goalViolationDetectionTimer;
  private final boolean _populateReplicaInfo = true;

  public IntraBrokerGoalViolationDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
    super(anomalies, kafkaCruiseControl, dropwizardMetricRegistry);
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    // Notice that we use a separate set of Goal instances for anomaly detector to avoid interference.
    _detectionGoals = config.getConfiguredInstances(AnomalyDetectorConfig.ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG, Goal.class);
    _goalViolationDetectionTimer = dropwizardMetricRegistry.timer(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR,
            "intra-broker-goal-violation-detection-timer"));

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
      IntraBrokerGoalViolations goalViolations = _kafkaCruiseControl.config().getConfiguredInstance(
              AnomalyDetectorConfig.INTRA_BROKER_GOAL_VIOLATIONS_CLASS_CONFIG,
              IntraBrokerGoalViolations.class,
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
                      new OperationProgress(),
                      true);

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

  protected boolean optimizeForGoal(ClusterModel clusterModel,
                                    Goal goal,
                                    IntraBrokerGoalViolations goalViolations,
                                    Set<Integer> excludedBrokersForLeadership,
                                    Set<Integer> excludedBrokersForReplicaMove,
                                    boolean checkPartitionsWithRFGreaterThanNumRacks)
          throws KafkaCruiseControlException {
    if (clusterModel.topics().isEmpty()) {
      LOG.info("Skipping goal violation detection because the cluster model does not have any topic.");
      return false;
    }

    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();
    try {
      OptimizationOptions options = _optimizationOptionsGenerator.optimizationOptionsForGoalViolationDetection(clusterModel,
              excludedTopics(clusterModel),
              excludedBrokersForLeadership,
              excludedBrokersForReplicaMove);
      if (checkPartitionsWithRFGreaterThanNumRacks) {
        _hasPartitionsWithRFGreaterThanNumRacks = clusterModel.maxReplicationFactor() > clusterModel.numAliveRacksAllowedReplicaMoves(options);
      }
      goal.optimize(clusterModel, Collections.emptySet(), options);
    } catch (OptimizationFailureException ofe) {
      // An OptimizationFailureException indicates (1) a hard goal violation that cannot be fixed typically due to
      // lack of physical hardware (e.g. insufficient number of racks to satisfy rack awareness, insufficient number
      // of brokers to satisfy Replica Capacity Goal, or insufficient number of resources to satisfy resource
      // capacity goals), or (2) a failure to move offline replicas away from dead brokers/disks.
      goalViolations.addViolation(goal.name(), false);
      return true;
    }
    boolean hasDiff = AnalyzerUtils.hasDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    LOG.trace("{} generated {} proposals", goal.name(), hasDiff ? "some" : "no");
    if (hasDiff) {
      // A goal violation that can be optimized by applying the generated proposals.
      goalViolations.addViolation(goal.name(), true);
      return true;
    } else {
      // The goal is already satisfied.
      return false;
    }
  }

}
