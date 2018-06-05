/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  /**
   * Construct the Cruise Control
   *
   * @param config the configuration of Cruise Control.
   */
  public KafkaCruiseControl(KafkaCruiseControlConfig config, MetricRegistry dropwizardMetricRegistry) {
    _config = config;
    _time = new SystemTime();
    // initialize some of the static state of Kafka Cruise Control;
    ModelUtils.init(config);
    ModelParameters.init(config);

    // Instantiate the components.
    _loadMonitor = new LoadMonitor(config, _time, dropwizardMetricRegistry, KafkaMetricDef.commonMetricDef());
    _goalOptimizerExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("GoalOptimizerExecutor", true, null));
    _goalOptimizer = new GoalOptimizer(config, _loadMonitor, _time, dropwizardMetricRegistry);
    _executor = new Executor(config, _time, dropwizardMetricRegistry);
    _anomalyDetector = new AnomalyDetector(config, _loadMonitor, this, _time, dropwizardMetricRegistry);
  }

  /**
   * Start up the Cruise Control.
   */
  public void startUp() {
    LOG.info("Starting Kafka Cruise Control...");
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
   * @param requirements The cluster model completeness requirements.
   * @param operationProgress the progress to report.
   * @return the optimization result.
   *
   * @throws KafkaCruiseControlException when any exception occurred during the decommission process.
   */
  public GoalOptimizer.OptimizerResult decommissionBrokers(Collection<Integer> brokerIds,
                                                           boolean dryRun,
                                                           boolean throttleDecommissionedBroker,
                                                           List<String> goals,
                                                           ModelCompletenessRequirements requirements,
                                                           OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    Map<Integer, Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements modelCompletenessRequirements =
        modelCompletenessRequirements(goalsByPriority.values()).weaker(requirements);
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(), modelCompletenessRequirements,
                                                            operationProgress);
      brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
      GoalOptimizer.OptimizerResult result = getOptimizationProposals(clusterModel, goalsByPriority, operationProgress);
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
   * @param requirements The cluster model completeness requirements.
   * @param operationProgress The progress of the job to update.
   * @return The optimization result.
   * @throws KafkaCruiseControlException when any exception occurred during the broker addition.
   */
  public GoalOptimizer.OptimizerResult addBrokers(Collection<Integer> brokerIds,
                                                  boolean dryRun,
                                                  boolean throttleAddedBrokers,
                                                  List<String> goals,
                                                  ModelCompletenessRequirements requirements,
                                                  OperationProgress operationProgress) throws KafkaCruiseControlException {
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      Map<Integer, Goal> goalsByPriority = goalsByPriority(goals);
      ModelCompletenessRequirements modelCompletenessRequirements =
          modelCompletenessRequirements(goalsByPriority.values()).weaker(requirements);
      ClusterModel clusterModel =
          _loadMonitor.clusterModel(_time.milliseconds(), modelCompletenessRequirements, operationProgress);
      brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.NEW));
      GoalOptimizer.OptimizerResult result = getOptimizationProposals(clusterModel, goalsByPriority, operationProgress);
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
   * @param operationProgress the progress of the job to report.
   * @return the optimization result.
   * @throws KafkaCruiseControlException when the rebalacnce encounter errors.
   */
  public GoalOptimizer.OptimizerResult rebalance(List<String> goals,
                                                 boolean dryRun,
                                                 ModelCompletenessRequirements requirements,
                                                 OperationProgress operationProgress) throws KafkaCruiseControlException {
    GoalOptimizer.OptimizerResult result = getOptimizationProposals(goals, requirements, operationProgress);
    if (!dryRun) {
      executeProposals(result.goalProposals(), Collections.emptySet());
    }
    return result;
  }

  /**
   * Demote given brokers by migrating the leaders from them to other brokers.
   *
   * The result of the broker demotion is not guaranteed to be able to move all the leaders away from the
   * given brokers. The operation is with best effort. There are various possibilities that some leaders
   * cannot be migrated (e.g. no other broker is in the ISR).
   *
   * Also, this method is stateless, i.e. a demoted broker will not remain in a demoted state after this
   * operation. If there is another broker failure, the leader may be moved to the demoted broker again
   * by Kafka controller.
   *
   * @param brokerIds the broker ids to move off.
   * @param dryRun whether it is a dry run or not
   * @param operationProgress the progress of the job to report.
   * @return the optimization result.
   */
  public GoalOptimizer.OptimizerResult demoteBrokers(Collection<Integer> brokerIds,
                                                     boolean dryRun,
                                                     OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal();
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(),
                                                            goal.clusterModelCompletenessRequirements(),
                                                            operationProgress);
      brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEMOTED));
      GoalOptimizer.OptimizerResult result =
          getOptimizationProposals(clusterModel,
                                   goalsByPriority(Collections.singletonList(goal.getClass().getSimpleName())),
                                   operationProgress);
      if (!dryRun) {
        executeProposals(result.goalProposals(), brokerIds);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
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
   * @param operationProgress the progress of the job to report.
   * @return the cluster workload model.
   * @throws KafkaCruiseControlException when the cluster model generation encounter errors.
   */
  public ClusterModel clusterModel(long now,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      return _loadMonitor.clusterModel(now, requirements, operationProgress);
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
   * @param requirements the model completeness requirement to enforce.
   * @param operationProgress the progress of the job to report.
   * @return the cluster workload model.
   * @throws KafkaCruiseControlException when the cluster model generation encounter errors.
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      return _loadMonitor.clusterModel(from, to, requirements, operationProgress);
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
   * @param operationProgress the job progress to report.
   * @return The optimization result.
   */
  public GoalOptimizer.OptimizerResult getOptimizationProposals(OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    try {
      return _goalOptimizer.optimizations(operationProgress);
    } catch (InterruptedException ie) {
      throw new KafkaCruiseControlException("Interrupted when getting the optimization proposals", ie);
    }
  }

  /**
   * Optimize a cluster workload model.
   * @param goals a list of goals to optimize. When empty all goals will be used.
   * @param requirements the model completeness requirements to enforce when generating the propsoals.
   * @param operationProgress the progress of the job to report.
   * @return The optimization result.
   * @throws KafkaCruiseControlException
   */
  public GoalOptimizer.OptimizerResult getOptimizationProposals(List<String> goals,
                                                                ModelCompletenessRequirements requirements,
                                                                OperationProgress operationProgress) throws KafkaCruiseControlException {
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
        || requirementsForCache.minRequiredNumWindows() > modelCompletenessRequirements.minRequiredNumWindows()
        || (requirementsForCache.includeAllTopics() && !modelCompletenessRequirements.includeAllTopics());
    if ((goals != null && !goals.isEmpty()) || hasWeakerRequirement) {
      try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
        // The cached proposals are computed with ignoreMinMonitoredPartitions = true. So if user provided a different
        // setting, we need to generate a new model.
        ClusterModel clusterModel =
            _loadMonitor.clusterModel(-1, _time.milliseconds(), modelCompletenessRequirements, operationProgress);
        result = getOptimizationProposals(clusterModel, goalsByPriority, operationProgress);
      } catch (KafkaCruiseControlException kcce) {
        throw kcce;
      } catch (Exception e) {
        throw new KafkaCruiseControlException(e);
      }
    } else {
      result = getOptimizationProposals(operationProgress);
    }
    return result;
  }

  private GoalOptimizer.OptimizerResult getOptimizationProposals(ClusterModel clusterModel,
                                                                 Map<Integer, Goal> goalsByPriority,
                                                                 OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    synchronized (this) {
      return _goalOptimizer.optimizations(clusterModel, goalsByPriority, operationProgress);
    }
  }

  /**
   * Execute the given balancing proposals.
   * @param proposals the given balancing proposals
   * @param unthrottledBrokers Brokers for which the rate of replica movements from/to will not be throttled.
   */
  public void executeProposals(Collection<ExecutionProposal> proposals,
                               Collection<Integer> unthrottledBrokers) {
    // Add execution proposals and start execution.
    _executor.executeProposals(proposals, unthrottledBrokers, _loadMonitor);
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
  public KafkaCruiseControlState state(OperationProgress operationProgress) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _loadMonitor.refreshClusterAndGeneration();
    return new KafkaCruiseControlState(_executor.state(),
                                       _loadMonitor.state(operationProgress, clusterAndGeneration),
                                       _goalOptimizer.state(clusterAndGeneration));
  }

  /**
   * Get the cluster state for Kafka.
   */
  public KafkaClusterState kafkaClusterState() {
    return new KafkaClusterState(_loadMonitor.kafkaCluster());
  }

  /**
   * Get the default model completeness requirement for Cruise Control. This is the combination of the
   * requirements of all the goals.
   */
  public ModelCompletenessRequirements defaultModelCompletenessRequirements() {
    return _goalOptimizer.defaultModelCompletenessRequirements();
  }

  private ModelCompletenessRequirements modelCompletenessRequirements(Collection<Goal> overrides) {
    return overrides == null || overrides.isEmpty() ?
        _goalOptimizer.defaultModelCompletenessRequirements() : MonitorUtils.combineLoadRequirementOptions(overrides);
  }

  /**
   * Check if the given goals meet the completeness requirements.
   *
   * @param goalNames Goal names (and empty list of names indicates all goals).
   */
  public boolean meetCompletenessRequirements(List<String> goalNames) {
    Collection<Goal> goals = goalsByPriority(goalNames).values();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _loadMonitor.refreshClusterAndGeneration();
    return goals.stream().allMatch(g -> _loadMonitor.meetCompletenessRequirements(
        clusterAndGeneration, g.clusterModelCompletenessRequirements()));
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
