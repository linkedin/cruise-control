/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The main class of Cruise Control.
 */
public class KafkaCruiseControl {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControl.class);
  private final KafkaCruiseControlConfig _config;
  private final LoadMonitor _loadMonitor;
  private final GoalOptimizer _goalOptimizer;
  private final ExecutorService _goalOptimizerExecutor;
  private final Executor _executor;
  private final AnomalyDetector _anomalyDetector;
  private final Time _time;
  private final MetricRegistry _dropwizardMetricRegistry;
  private final JmxReporter _reporter;
  private final String _metricsPrefix = "kafka.cruisecontrol";

  /**
   * Construct the Cruise Control
   *
   * @param config the configuration of Cruise Control.
   */
  public KafkaCruiseControl(KafkaCruiseControlConfig config) {
    _config = config;
    _time = new SystemTime();
    // initialize some of the static state of Kafka Cruise Control;
    Load.init(config);
    ModelUtils.init(config);
    ModelParameters.init(config);
    _dropwizardMetricRegistry = new MetricRegistry();
    _reporter = JmxReporter.forRegistry(_dropwizardMetricRegistry).inDomain(_metricsPrefix).build();

    // Instantiate the components.
    _loadMonitor = new LoadMonitor(config, _time, _dropwizardMetricRegistry);
    _goalOptimizerExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("GoalOptimizerExecutor", true, null));
    _goalOptimizer = new GoalOptimizer(config, _loadMonitor, _time, _dropwizardMetricRegistry);
    _executor = new Executor(config, _time, _dropwizardMetricRegistry);
    _anomalyDetector = new AnomalyDetector(config, _loadMonitor, this, _time, _dropwizardMetricRegistry);
  }

  /**
   * Start up the Cruise Control.
   */
  public void startUp() {
    LOG.info("Starting Kafka Cruise Control...");
    _reporter.start();
    _loadMonitor.startUp();
    _anomalyDetector.startDetection();
    _goalOptimizerExecutor.submit(_goalOptimizer);
    LOG.info("Kafka Cruise Control started.");
  }

  public void shutdown() {
    Thread t = new Thread() {
      @Override
      public void run() {
        LOG.info("Shutting down Kafka Cruise Control...");
        _loadMonitor.shutdown();
        _executor.shutdown();
        _anomalyDetector.shutdown();
        _goalOptimizer.shutdown();
        _reporter.close();
        LOG.info("Kafka Cruise Control shutdown completed.");
      }
    };
    t.setDaemon(true);
    t.start();
    try {
      t.join(30000);
    } catch (InterruptedException e) {
      LOG.warn("Cruise Control failed to shutdown in 30 seconds. Exit.");
    }
  }

  /**
   * Decommission a broker.
   *
   * @param brokerIds The brokers to decommission.
   * @param dryRun throw
   * @param throttleDecommissionedBroker whether throttle the brokers that are being decommissioned.
   * @param goals the goals to be met when decommissioning the brokers. When empty all goals will be used.
   * @return the optimization result.
   *
   * @throws KafkaCruiseControlException when any exception occurred during the decommission process.
   */
  public GoalOptimizer.OptimizerResult decommissionBrokers(Collection<Integer> brokerIds,
                                                           boolean dryRun,
                                                           boolean throttleDecommissionedBroker,
                                                           List<String> goals,
                                                           ModelCompletenessRequirements requirements)
      throws KafkaCruiseControlException {
    Map<Integer, Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements modelCompletenessRequirements =
        modelCompletenessRequirements(goalsByPriority.values()).weaker(requirements);
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration()) {
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(), modelCompletenessRequirements);
      brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
      GoalOptimizer.OptimizerResult result = getOptimizationProposals(clusterModel, goalsByPriority);
      if (!dryRun) {
        executeProposals(result.goalProposals(), throttleDecommissionedBroker ? Collections.emptyList() : brokerIds);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Add brokers
   * @param brokerIds the broker ids.
   * @param dryRun whether it is a dry run or not.
   * @param throttleAddedBrokers whether throttle the brokers that are being added.
   * @param goals the goals to be met when adding the brokers. When empty all goals will be used.
   * @return The optimization result.
   * @throws KafkaCruiseControlException when any exception occurred during the broker addition.
   */
  public GoalOptimizer.OptimizerResult addBrokers(Collection<Integer> brokerIds,
                                                  boolean dryRun,
                                                  boolean throttleAddedBrokers,
                                                  List<String> goals,
                                                  ModelCompletenessRequirements requirements) throws KafkaCruiseControlException {
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration()) {
      Map<Integer, Goal> goalsByPriority = goalsByPriority(goals);
      ModelCompletenessRequirements modelCompletenessRequirements =
          modelCompletenessRequirements(goalsByPriority.values()).weaker(requirements);
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(), modelCompletenessRequirements);
      brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.NEW));
      GoalOptimizer.OptimizerResult result = getOptimizationProposals(clusterModel, goalsByPriority);
      if (!dryRun) {
        executeProposals(result.goalProposals(), throttleAddedBrokers ? Collections.emptyList() : brokerIds);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Rebalance the cluster
   * @param goals the goals to be met during the rebalance. When empty all goals will be used.
   * @param dryRun whether it is a dry run or not.
   * @param requirements The cluster model completeness requirements.
   * @return the optimization result.
   * @throws KafkaCruiseControlException when the rebalacnce encounter errors.
   */
  public GoalOptimizer.OptimizerResult rebalance(List<String> goals,
                                                 boolean dryRun,
                                                 ModelCompletenessRequirements requirements) throws KafkaCruiseControlException {
    GoalOptimizer.OptimizerResult result = getOptimizationProposals(goals, requirements);
    if (!dryRun) {
      executeProposals(result.goalProposals(), Collections.emptySet());
    }
    return result;
  }

  /**
   * Get the broker load stats from the cache. null will be returned if their is no cached broker load stats.
   */
  public ClusterModel.BrokerStats cachedBrokerLoadStats() {
    return _loadMonitor.cachedBrokerLoadStats();
  }

  /**
   * Get the cluster model cutting off at a certain timestamp.
   * @param now time.
   * @param requirements the model completeness requirements.
   * @return the cluster workload model.
   * @throws KafkaCruiseControlException when the cluster model generation encounter errors.
   */
  public ClusterModel clusterModel(long now, ModelCompletenessRequirements requirements)
      throws KafkaCruiseControlException {
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration()) {
      return _loadMonitor.clusterModel(now, requirements);
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Get the cluster model for a given time window.
   * @param from the start time of the window
   * @param to the end time of the window
   * @return the cluster workload model.
   * @throws KafkaCruiseControlException when the cluster model generation encounter errors.
   */
  public ClusterModel clusterModel(long from, long to, ModelCompletenessRequirements modelCompletenessRequirements)
      throws KafkaCruiseControlException {
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration()) {
      return _loadMonitor.clusterModel(from, to, modelCompletenessRequirements);
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
        throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Bootstrap the load monitor for a given period.
   * @param startMs the starting time of the bootstrap period.
   * @param endMs the end time of the bootstrap period.
   * @param clearMetrics clear the existing metrics.
   */
  public void bootstrapLoadMonitor(long startMs, long endMs, boolean clearMetrics) {
    _loadMonitor.bootstrap(startMs, endMs, clearMetrics);
  }

  /**
   * Bootstrap the load monitor from the given timestamp until it catches up.
   * This method clears all existing metric samples.
   *
   * @param startMs the starting time of the bootstrap period.
   * @param clearMetrics clear the existing metric samples
   */
  public void bootstrapLoadMonitor(long startMs, boolean clearMetrics) {
    _loadMonitor.bootstrap(startMs, clearMetrics);
  }

  /**
   * Bootstrap the load monitor with the most recent metric samples until it catches up.
   * This method clears all existing metric samples.
   *
   * @param clearMetrics clear the existing metric samples
   */
  public void bootstrapLoadMonitor(boolean clearMetrics) {
    _loadMonitor.bootstrap(clearMetrics);
  }

  /**
   * Train load model of Kafka Cruise Control with metric samples in a training period.
   * @param startMs the starting time of the training period.
   * @param endMs the end time of the training period.
   */
  public void trainLoadModel(long startMs, long endMs) {
    _loadMonitor.train(startMs, endMs);
  }

  /**
   * Pause all the activities of the load monitor. Load monitor can only be paused if it is in RUNNING state.
   */
  public void pauseLoadMonitorActivity() {
    _loadMonitor.pauseMetricSampling();
  }

  /**
   * Resume all the activities of the load monitor.
   */
  public void resumeLoadMonitorActivity() {
    _loadMonitor.resumeMetricSampling();
  }

  /**
   * Get the optimization proposals from the current cluster. The result would be served from the cached result if
   * it is still valid.
   * @return The optimization result.
   * @throws KafkaCruiseControlException
   * @throws AnalysisInputException
   */
  public GoalOptimizer.OptimizerResult getOptimizationProposals() throws KafkaCruiseControlException {
      return _goalOptimizer.optimizations();
  }

  /**
   * Optimize a cluster workload model.
   * @param goals a list of goals to optimize. When empty all goals will be used.
   * @return The optimization result.
   * @throws KafkaCruiseControlException
   */
  public GoalOptimizer.OptimizerResult getOptimizationProposals(List<String> goals,
                                                                ModelCompletenessRequirements requirements) throws KafkaCruiseControlException {
    GoalOptimizer.OptimizerResult result;
    Map<Integer, Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements modelCompletenessRequirements =
        modelCompletenessRequirements(goalsByPriority.values()).weaker(requirements);
    // There are a few cases that we cannot use the cached best proposals:
    // 1. When users specified goals.
    // 2. When provided requirements contains a weaker requirement than what is used by the cached proposal.
    ModelCompletenessRequirements requirementsForCache = _goalOptimizer.modelCompletenessRequirementsForPrecomputing();
    boolean hasWeakerRequirement =
        requirementsForCache.minMonitoredPartitionsPercentage() > modelCompletenessRequirements.minMonitoredPartitionsPercentage()
        || requirementsForCache.minRequiredNumSnapshotWindows() > modelCompletenessRequirements.minRequiredNumSnapshotWindows()
        || (requirementsForCache.includeAllTopics() && !modelCompletenessRequirements.includeAllTopics());
    if ((goals != null && !goals.isEmpty()) || hasWeakerRequirement) {
      try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration()) {
        // The cached proposals are computed with ignoreMinMonitoredPartitions = true. So if user provided a different
        // setting, we need to generate a new model.
        ClusterModel clusterModel = _loadMonitor.clusterModel(-1, _time.milliseconds(), modelCompletenessRequirements);
        result = getOptimizationProposals(clusterModel, goalsByPriority);
      } catch (KafkaCruiseControlException kcce) {
        throw kcce;
      } catch (Exception e) {
        throw new KafkaCruiseControlException(e);
      }
    } else {
      result = getOptimizationProposals();
    }
    return result;
  }

  private GoalOptimizer.OptimizerResult getOptimizationProposals(ClusterModel clusterModel,
                                                                 Map<Integer, Goal> goalsByPriority)
      throws KafkaCruiseControlException {
    synchronized (this) {
      return _goalOptimizer.optimizations(clusterModel, goalsByPriority);
    }
  }

  /**
   * Execute the given balancing proposals.
   * @param proposals the given balancing proposals
   * @param unthrottledBrokers Brokers for which the rate of replica movements from/to will not be throttled.
   */
  public void executeProposals(Collection<BalancingProposal> proposals,
                               Collection<Integer> unthrottledBrokers) {
    // Pause the load monitor before the proposal execution.
    _loadMonitor.pauseMetricSampling();
    _executor.addBalancingProposals(proposals, unthrottledBrokers);
    _executor.startExecution(_loadMonitor);
  }

  /**
   * Stop the executor if it is executing the proposals.
   */
  public void stopProposalExecution() {
    _executor.stopExecution();
  }

  /**
   * Get the state for Kafka Cruise Control.
   */
  public KafkaCruiseControlState state() {
    return new KafkaCruiseControlState(_executor.state(), _loadMonitor.state(), _goalOptimizer.state());
  }

  /**
   * Get the default model completeness requirement for Cruise Control. This is the combination of the
   * requirements of all the goals.
   */
  public ModelCompletenessRequirements defaultModelCompletenessRequirements() {
    return _goalOptimizer.defaultModelCompletenessRequirements();
  }

  /**
   * Get the current snapshots for Cruise Control.  This is basically a data dump.
   * @return a non-null map of the current snapshots for all partitions.
   */
  public SortedMap<Long, Map<TopicPartition, Snapshot>> currentSnapshots() {
    return _loadMonitor.currentSnapshots();
  }

  private ModelCompletenessRequirements modelCompletenessRequirements(Collection<Goal> overrides) {
    return overrides == null || overrides.isEmpty() ?
        _goalOptimizer.defaultModelCompletenessRequirements() : MonitorUtils.combineLoadRequirementOptions(overrides);
  }

  /**
   * Get a goals by priority based on the goal list.
   */
  private Map<Integer, Goal> goalsByPriority(List<String> goals) {
    if (goals == null || goals.isEmpty()) {
      return AnalyzerUtils.getGoalMapByPriority(_config);
    }
    Map<String, Goal> allGoals = AnalyzerUtils.getCaseInsensitiveGoalsByName(_config);
    Map<Integer, Goal> goalsByPriority = new HashMap<>();
    int i = 0;
    for (String goalName : goals) {
      Goal goal = allGoals.get(goalName);
      if (goal == null) {
        throw new IllegalArgumentException("Goal with name " + goalName + " does not exist.");
      }
      goalsByPriority.put(i++, allGoals.get(goalName));
    }
    return goalsByPriority;
  }
}
