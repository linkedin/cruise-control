/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.NotEnoughValidSnapshotsException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class will be schedule to run periodically to check if the given goals are violated or not. An alert will be
 * triggered if one of the goal is not met.
 */
public class GoalViolationDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolationDetector.class);
  private final LoadMonitor _loadMonitor;
  private final SortedMap<Integer, Goal> _goals;
  private final Time _time;
  private final Queue<Anomaly> _anomalies;
  private ModelGeneration _lastCheckedModelGeneration;

  public GoalViolationDetector(KafkaCruiseControlConfig config,
                               LoadMonitor loadMonitor,
                               Queue<Anomaly> anomalies,
                               Time time) {
    _loadMonitor = loadMonitor;
    // Notice that we use a separate set of Goal instances for anomaly detector to avoid interference.
    _goals = getDetectorGoalsMap(config);
    _anomalies = anomalies;
    _time = time;
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
    long now = _time.milliseconds();
    if (_loadMonitor.clusterModelGeneration().equals(_lastCheckedModelGeneration)) {
      LOG.debug("Skipping goal violation detection because the model generation hasn't changed. Current model generation {}",
                _loadMonitor.clusterModelGeneration());
      return;
    }
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration()) {
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
      if (loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING ||
          loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING) {
        LOG.info("Skipping goal violation detection because load monitor is in {} state", loadMonitorTaskRunnerState);
        return;
      }

      GoalViolations goalViolations = new GoalViolations();
      boolean newModelNeeded = true;
      ClusterModel clusterModel = null;
      for (Map.Entry<Integer, Goal> entry : _goals.entrySet()) {
        int priority = entry.getKey();
        Goal goal = entry.getValue();
        if (_loadMonitor.meetCompletenessRequirements(goal.clusterModelCompletenessRequirements())) {
          LOG.debug("Detecting if {} is violated.", entry.getValue().name());
          // Because the model generation could be slow, We only get new cluster model if needed.
          if (newModelNeeded) {
            // Make cluster model null before generating a new cluster model so the current one can be GCed.
            clusterModel = null;
            clusterModel = _loadMonitor.clusterModel(now, goal.clusterModelCompletenessRequirements());
            // The anomaly detector have to include all the topics in order to detect the rack awareness issue.
            newModelNeeded = false;
          }
          if (clusterModel.topics().isEmpty()) {
            LOG.info("Skipping goal violation detection because the cluster model does not have any topic.");
            return;
          }
          Map<TopicPartition, List<Integer>> initDistribution = clusterModel.getReplicaDistribution();
          // We do not exclude any topics when we are doing anomaly detection.
          goal.optimize(clusterModel, new HashSet<>(), Collections.emptySet());
          Set<BalancingProposal> proposals = AnalyzerUtils.getDiff(initDistribution, clusterModel);
          LOG.trace("{} generated {} proposals", goal.name(), proposals.size());
          if (!proposals.isEmpty()) {
            goalViolations.addViolation(priority, goal.name(), proposals);
            newModelNeeded = true;
          }
        } else {
          LOG.debug("Skipping goal violation for {} detection because load completeness requirement is not met.",
                    goal.name());
        }
      }
      if (clusterModel != null) {
        _lastCheckedModelGeneration = clusterModel.generation();
      }
      if (!goalViolations.violations().isEmpty()) {
        _anomalies.add(goalViolations);
      }
    } catch (NotEnoughValidSnapshotsException nempe) {
      LOG.debug("Skipping goal violation detection because there are not enough modeled partitions.");
    } catch (KafkaCruiseControlException kcce) {
      LOG.warn("Goal violation detector received exception", kcce);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      LOG.debug("Goal violation detection finished.");
    }
  }
}
