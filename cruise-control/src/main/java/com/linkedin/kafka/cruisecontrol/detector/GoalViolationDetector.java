/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.balancednessCostByGoal;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.MAX_BALANCEDNESS_SCORE;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckCapacityEstimation;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.shouldSkipAnomalyDetection;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


/**
 * This class will be scheduled to run periodically to check if the given goals are violated or not. An alert will be
 * triggered if one of the goals is not met.
 */
public class GoalViolationDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolationDetector.class);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final List<Goal> _detectionGoals;
  private final Queue<Anomaly> _anomalies;
  private ModelGeneration _lastCheckedModelGeneration;
  private final Pattern _excludedTopics;
  private final boolean _allowCapacityEstimation;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final Map<String, Double> _balancednessCostByGoal;
  private volatile double _balancednessScore;

  public GoalViolationDetector(Queue<Anomaly> anomalies,
                               KafkaCruiseControl kafkaCruiseControl) {
    KafkaCruiseControlConfig config = kafkaCruiseControl.config();
    // Notice that we use a separate set of Goal instances for anomaly detector to avoid interference.
    _detectionGoals = config.getConfiguredInstances(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG, Goal.class);
    _anomalies = anomalies;
    _excludedTopics = Pattern.compile(config.getString(AnalyzerConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG));
    _allowCapacityEstimation = config.getBoolean(AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    _excludeRecentlyDemotedBrokers = config.getBoolean(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    _excludeRecentlyRemovedBrokers = config.getBoolean(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    _kafkaCruiseControl = kafkaCruiseControl;
    _balancednessCostByGoal = balancednessCostByGoal(_detectionGoals,
                                                     config.getDouble(AnalyzerConfig.GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG),
                                                     config.getDouble(AnalyzerConfig.GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG));
    _balancednessScore = MAX_BALANCEDNESS_SCORE;
  }

  /**
   * @return A metric to quantify how well the load distribution on a cluster satisfies the {@link #_detectionGoals}.
   */
  public double balancednessScore() {
    return _balancednessScore;
  }

  /**
   * Skip goal violation detection if any of the following is true:
   * <ul>
   * <li>Cluster model generation has not changed since the last goal violation check.</li>
   * <li>There is offline replicas in the cluster, which means there is dead brokers/disks. In this case
   * {@link BrokerFailureDetector} or {@link DiskFailureDetector} should take care of the anomaly.</li>
   * <li>{@link AnomalyDetectorUtils#shouldSkipAnomalyDetection(KafkaCruiseControl)} returns true.
   * </ul>
   *
   * @return True to skip goal violation detection based on the current state, false otherwise.
   */
  private boolean shouldSkipGoalViolationDetection() {
    if (_kafkaCruiseControl.loadMonitor().clusterModelGeneration().equals(_lastCheckedModelGeneration)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping goal violation detection because the model generation hasn't changed. Current model generation {}",
                  _kafkaCruiseControl.loadMonitor().clusterModelGeneration());
      }
      return true;
    }

    Set<Integer> brokersWithOfflineReplicas = _kafkaCruiseControl.loadMonitor().brokersWithOfflineReplicas(MAX_METADATA_WAIT_MS);
    if (!brokersWithOfflineReplicas.isEmpty()) {
      LOG.info("Skipping goal violation detection because there are dead brokers/disks in the cluster, flawed brokers: {}",
                brokersWithOfflineReplicas);
      setBalancednessWithOfflineReplicas();
      return true;
    }

    return shouldSkipAnomalyDetection(_kafkaCruiseControl);
  }

  @Override
  public void run() {
    if (shouldSkipGoalViolationDetection()) {
      return;
    }

    AutoCloseable clusterModelSemaphore = null;
    try {
      Map<String, Object> parameterConfigOverrides = new HashMap<>(2);
      parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
      parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
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
                                                            new OperationProgress());

            // If the clusterModel contains dead brokers or disks, goal violation detector will ignore any goal violations.
            // Detection and fix for dead brokers/disks is the responsibility of broker/disk failure detector.
            if (skipDueToOfflineReplicas(clusterModel)) {
              return;
            }
            sanityCheckCapacityEstimation(_allowCapacityEstimation, clusterModel.capacityEstimationInfoByBrokerId());
            _lastCheckedModelGeneration = clusterModel.generation();
          }
          newModelNeeded = optimizeForGoal(clusterModel, goal, goalViolations, excludedBrokersForLeadership, excludedBrokersForReplicaMove);
        } else {
          LOG.warn("Skipping goal violation detection for {} because load completeness requirement is not met.", goal);
        }
      }
      Map<Boolean, List<String>> violatedGoalsByFixability = goalViolations.violatedGoalsByFixability();
      if (!violatedGoalsByFixability.isEmpty()) {
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

  /**
   * @param clusterModel The state of the cluster.
   * @return True to skip goal violation detection due to offline replicas in the cluster model.
   */
  private boolean skipDueToOfflineReplicas(ClusterModel clusterModel) {
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

  private void setBalancednessWithOfflineReplicas() {
    _balancednessScore = 0.0;
  }

  private void refreshBalancednessScore(Map<Boolean, List<String>> violatedGoalsByFixability) {
    _balancednessScore = MAX_BALANCEDNESS_SCORE;
    for (List<String> violatedGoals : violatedGoalsByFixability.values()) {
      violatedGoals.forEach(violatedGoal -> _balancednessScore -= _balancednessCostByGoal.get(violatedGoal));
    }
  }

  private Set<String> excludedTopics(ClusterModel clusterModel) {
    return clusterModel.topics()
        .stream()
        .filter(topic -> _excludedTopics.matcher(topic).matches())
        .collect(Collectors.toSet());
  }

  private boolean optimizeForGoal(ClusterModel clusterModel,
                                  Goal goal,
                                  GoalViolations goalViolations,
                                  Set<Integer> excludedBrokersForLeadership,
                                  Set<Integer> excludedBrokersForReplicaMove)
      throws KafkaCruiseControlException {
    if (clusterModel.topics().isEmpty()) {
      LOG.info("Skipping goal violation detection because the cluster model does not have any topic.");
      return false;
    }
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();
    try {
      goal.optimize(clusterModel, new HashSet<>(), new OptimizationOptions(excludedTopics(clusterModel),
                                                                           excludedBrokersForLeadership,
                                                                           excludedBrokersForReplicaMove,
                                                                           true));
    } catch (OptimizationFailureException ofe) {
      // An OptimizationFailureException indicates (1) a hard goal violation that cannot be fixed typically due to
      // lack of physical hardware (e.g. insufficient number of racks to satisfy rack awareness, insufficient number
      // of brokers to satisfy Replica Capacity Goal, or insufficient number of resources to satisfy resource
      // capacity goals), or (2) a failure to move offline replicas away from dead brokers/disks.
      goalViolations.addViolation(goal.name(), false);
      return true;
    }
    Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    LOG.trace("{} generated {} proposals", goal.name(), proposals.size());
    if (!proposals.isEmpty()) {
      // A goal violation that can be optimized by applying the generated proposals.
      goalViolations.addViolation(goal.name(), true);
      return true;
    } else {
      // The goal is already satisfied.
      return false;
    }
  }
}
