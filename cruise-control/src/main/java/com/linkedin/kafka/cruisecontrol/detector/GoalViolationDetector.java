/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class will be scheduled to run periodically to check if the given goals are violated or not. An alert will be
 * triggered if one of the goals is not met.
 */
public class GoalViolationDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolationDetector.class);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final LoadMonitor _loadMonitor;
  private final SortedMap<Integer, Goal> _goals;
  private final Time _time;
  private final Queue<Anomaly> _anomalies;
  private ModelGeneration _lastCheckedModelGeneration;
  private final Pattern _excludedTopics;
  private final boolean _allowCapacityEstimation;

  public GoalViolationDetector(KafkaCruiseControlConfig config,
                               LoadMonitor loadMonitor,
                               Queue<Anomaly> anomalies,
                               Time time,
                               KafkaCruiseControl kafkaCruiseControl) {
    _loadMonitor = loadMonitor;
    // Notice that we use a separate set of Goal instances for anomaly detector to avoid interference.
    _goals = getDetectorGoalsMap(config);
    _anomalies = anomalies;
    _time = time;
    _excludedTopics = Pattern.compile(config.getString(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG));
    _allowCapacityEstimation = config.getBoolean(KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    _kafkaCruiseControl = kafkaCruiseControl;
  }

  private SortedMap<Integer, Goal> getDetectorGoalsMap(KafkaCruiseControlConfig config) {
    List<String> allGoals = config.getList(KafkaCruiseControlConfig.GOALS_CONFIG);
    Map<String, Integer> priorityMap = new HashMap<>();
    int i = 0;
    for (String goalClass : allGoals) {
      priorityMap.put(goalClass, i++);
    }
    List<Goal> goals = config.getConfiguredInstances(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG, Goal.class);
    SortedMap<Integer, Goal> orderedGoals = new TreeMap<>();
    for (Goal goal: goals) {
      Integer priority = priorityMap.get(goal.getClass().getName());
      if (priority == null) {
        throw new IllegalArgumentException(goal.getClass().getName() + " is defined in " + KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG
                                               + " but not found in " + KafkaCruiseControlConfig.GOALS_CONFIG);
      }
      orderedGoals.put(priorityMap.get(goal.getClass().getName()), goal);
    }
    return orderedGoals;
  }

  @Override
  public void run() {
    if (_loadMonitor.clusterModelGeneration().equals(_lastCheckedModelGeneration)) {
      LOG.debug("Skipping goal violation detection because the model generation hasn't changed. Current model generation {}",
                _loadMonitor.clusterModelGeneration());
      return;
    }

    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
    if (!ViolationUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
      LOG.info("Skipping goal violation detection because load monitor is in {} state.", loadMonitorTaskRunnerState);
      return;
    }

    List<Goal> readyGoals =  _goals.values().stream()
                                   .filter(goal -> _loadMonitor.meetCompletenessRequirements(goal.clusterModelCompletenessRequirements()))
                                   .collect(Collectors.toList());
    if (!readyGoals.isEmpty()) {
      LOG.debug("Detecting if ready goals {} out of {} are violated.", readyGoals, _goals.values());
      GoalViolations goalViolations = new GoalViolations(_kafkaCruiseControl, _allowCapacityEstimation);
      ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(0, 0, false);
      for (Goal goal: readyGoals) {
        requirements = requirements.stronger(goal.clusterModelCompletenessRequirements());
      }
      ClusterModel clusterModel;
      try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(new OperationProgress())) {
        clusterModel = _loadMonitor.clusterModel(_time.milliseconds(),
            requirements,
            new OperationProgress());
        _lastCheckedModelGeneration = clusterModel.generation();
      }  catch (Exception e) {
        LOG.error("Unexpected exception when generating cluster model.", e);
        return;
      }

      for (Goal goal: readyGoals) {
        try {
          optimizeForGoal(clusterModel, goal, goalViolations);
        } catch (KafkaCruiseControlException kcce) {
          LOG.warn("Goal violation detector received exception", kcce);
        }
      }
      if (!goalViolations.violations().isEmpty()) {
        _anomalies.add(goalViolations);
      }
      LOG.debug("Goal violation detection finished.");
    }
  }

  private Set<String> excludedTopics(ClusterModel clusterModel) {
    return clusterModel.topics()
        .stream()
        .filter(topic -> _excludedTopics.matcher(topic).matches())
        .collect(Collectors.toSet());
  }

  private void optimizeForGoal(ClusterModel clusterModel,
                                  Goal goal,
                                  GoalViolations goalViolations)
      throws KafkaCruiseControlException {
    if (clusterModel.topics().isEmpty()) {
      LOG.info("Skipping goal violation detection because the cluster model does not have any topic.");
      return;
    }
    Map<TopicPartition, List<Integer>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, Integer> initLeaderDistribution = clusterModel.getLeaderDistribution();
    try {
      goal.optimize(clusterModel, new HashSet<>(), excludedTopics(clusterModel));
    } catch (OptimizationFailureException ofe) {
      // An OptimizationFailureException indicates (1) a hard goal violation that cannot be fixed typically due to
      // lack of physical hardware (e.g. insufficient number of racks to satisfy rack awareness, insufficient number
      // of brokers to satisfy Replica Capacity Goal, or insufficient number of resources to satisfy resource
      // capacity goals), or (2) a failure to move offline replicas away from dead brokers/disks.
      goalViolations.addViolation(goal.name(), false);
      return;
    }
    Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    LOG.trace("{} generated {} proposals", goal.name(), proposals.size());
    if (!proposals.isEmpty()) {
      // A goal violation that can be optimized by applying the generated proposals.
      goalViolations.addViolation(goal.name(), true);
    }
  }
}
