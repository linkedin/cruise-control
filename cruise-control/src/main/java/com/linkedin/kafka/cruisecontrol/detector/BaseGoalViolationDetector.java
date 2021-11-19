/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptionsGenerator;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.Utils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.*;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyDetectionStatus;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG;


/**
 * This class will be scheduled to run periodically to check if the given goals are violated or not. An alert will be
 * triggered if one of the goals is not met.
 */
abstract class BaseGoalViolationDetector extends AbstractAnomalyDetector implements Runnable {
  protected static final Logger LOG = LoggerFactory.getLogger(BaseGoalViolationDetector.class);
  protected final List<Goal> _detectionGoals;
  protected ModelGeneration _lastCheckedModelGeneration;
  protected final Pattern _excludedTopics;
  protected final boolean _allowCapacityEstimation;
  protected final boolean _excludeRecentlyDemotedBrokers;
  protected final boolean _excludeRecentlyRemovedBrokers;
  protected final Map<String, Double> _balancednessCostByGoal;
  protected volatile double _balancednessScore;
  protected volatile ProvisionResponse _provisionResponse;
  protected volatile boolean _hasPartitionsWithRFGreaterThanNumRacks;
  protected final OptimizationOptionsGenerator _optimizationOptionsGenerator;
  protected final Timer _goalViolationDetectionTimer;
  protected final Meter _automatedRightsizingMeter;
  protected static final double BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS = -1.0;
  protected final Provisioner _provisioner;
  protected final Boolean _isProvisionerEnabled;
  protected final boolean _populateReplicaInfo = false;

  BaseGoalViolationDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
    super(anomalies, kafkaCruiseControl);
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    // Notice that we use a separate set of Goal instances for anomaly detector to avoid interference.
    _detectionGoals = config.getConfiguredInstances(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG, Goal.class);
    _excludedTopics = Pattern.compile(config.getString(AnalyzerConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG));
    _allowCapacityEstimation = config.getBoolean(AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    _excludeRecentlyDemotedBrokers = config.getBoolean(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    _excludeRecentlyRemovedBrokers = config.getBoolean(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    _balancednessCostByGoal = balancednessCostByGoal(_detectionGoals,
            config.getDouble(AnalyzerConfig.GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG),
            config.getDouble(AnalyzerConfig.GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG));
    _balancednessScore = MAX_BALANCEDNESS_SCORE;
    _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
    _hasPartitionsWithRFGreaterThanNumRacks = false;
    Map<String, Object> overrideConfigs = Map.of(KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG, config,
            ADMIN_CLIENT_CONFIG, _kafkaCruiseControl.adminClient());
    _optimizationOptionsGenerator = config.getConfiguredInstance(AnalyzerConfig.OPTIMIZATION_OPTIONS_GENERATOR_CLASS_CONFIG,
            OptimizationOptionsGenerator.class,
            overrideConfigs);
    _goalViolationDetectionTimer = dropwizardMetricRegistry.timer(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR,
            "goal-violation-detection-timer"));
    _automatedRightsizingMeter = dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "automated-rightsizing-rate"));
    _provisioner = kafkaCruiseControl.provisioner();
    _isProvisionerEnabled = config.getBoolean(AnomalyDetectorConfig.PROVISIONER_ENABLE_CONFIG);
  }

  /**
   * @return A metric to quantify how well the load distribution on a cluster satisfies the {@link #_detectionGoals}.
   */
  public double balancednessScore() {
    return _balancednessScore;
  }

  /**
   * @return Provision status of the cluster based on the latest goal violation check.
   */
  public ProvisionStatus provisionStatus() {
    return _provisionResponse.status();
  }

  /**
   * @return {@code true} if the goal violation detector identified partitions with a replication factor (RF) greater than the number of
   * racks that contain brokers that are eligible to host replicas (i.e. not excluded for replica moves), {@code false} otherwise.
   */
  public boolean hasPartitionsWithRFGreaterThanNumRacks() {
    return _hasPartitionsWithRFGreaterThanNumRacks;
  }

  /**
   * Retrieve the {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the goal violation detector
   * is ready to check for an anomaly.
   *
   * <ul>
   *   <li>Skips detection if cluster model generation has not changed since the last goal violation check.</li>
   *   <li>In case the cluster has offline replicas, this function skips goal violation check and calls
   *   {@link #setBalancednessWithOfflineReplicas}.</li>
   *   <li>See {@link AnomalyDetectionStatus} for details.</li>
   * </ul>
   *
   * @return The {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the anomaly detector is ready.
   */
  protected AnomalyDetectionStatus getGoalViolationDetectionStatus() {
    if (_kafkaCruiseControl.loadMonitor().clusterModelGeneration().equals(_lastCheckedModelGeneration)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping goal violation detection because the model generation hasn't changed. Current model generation {}",
                _kafkaCruiseControl.loadMonitor().clusterModelGeneration());
      }
      return AnomalyDetectionStatus.SKIP_MODEL_GENERATION_NOT_CHANGED;
    }

    AnomalyDetectionStatus detectionStatus = getAnomalyDetectionStatus(_kafkaCruiseControl, true, true);
    if (detectionStatus == AnomalyDetectionStatus.SKIP_HAS_OFFLINE_REPLICAS) {
      setBalancednessWithOfflineReplicas();
    } else if (detectionStatus == AnomalyDetectionStatus.SKIP_EXECUTOR_NOT_READY) {
      // An ongoing execution might indicate a cluster expansion/shrinking. Hence, the detector avoids reporting a stale provision status.
      _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
      // An ongoing execution may modify the replication factor of partitions; hence, the detector avoids reporting potential false positives.
      _hasPartitionsWithRFGreaterThanNumRacks = false;
    }

    return detectionStatus;
  }

  /**
   * @param clusterModel The state of the cluster.
   * @return {@code true} to skip goal violation detection due to offline replicas in the cluster model.
   */
  protected boolean skipDueToOfflineReplicas(ClusterModel clusterModel) {
    if (!clusterModel.deadBrokers().isEmpty()) {
      LOG.info("Skipping goal violation detection due to dead brokers {}, which are reported by broker failure "
              + "detector, and fixed if its self healing configuration is enabled.", clusterModel.deadBrokers());
      setBalancednessWithOfflineReplicas();
      return true;
    } else if (!clusterModel.brokersWithBadDisks().isEmpty()) {
      LOG.info("Skipping goal violation detection due to brokers with bad disks {}, which are reported by disk failure "
              + "detector, and fixed if its self healing configuration is enabled.", clusterModel.brokersWithBadDisks());
      setBalancednessWithOfflineReplicas();
      return true;
    }

    return false;
  }

  protected void setBalancednessWithOfflineReplicas() {
    _balancednessScore = BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS;
    _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
  }

  protected void refreshBalancednessScore(Map<Boolean, List<String>> violatedGoalsByFixability) {
    _balancednessScore = MAX_BALANCEDNESS_SCORE;
    for (List<String> violatedGoals : violatedGoalsByFixability.values()) {
      violatedGoals.forEach(violatedGoal -> _balancednessScore -= _balancednessCostByGoal.get(violatedGoal));
    }
  }

  protected Set<String> excludedTopics(ClusterModel clusterModel) {
    return Utils.getTopicNamesMatchedWithPattern(_excludedTopics, clusterModel::topics);
  }

  protected boolean optimizeForGoal(ClusterModel clusterModel,
                                    Goal goal,
                                    GoalViolations goalViolations,
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
