/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.async.progress.OptimizationForGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.balancednessCostByGoal;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckCapacityEstimation;
import static com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING;
import static com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING;


/**
 * A class for optimizing goals in the given order of priority.
 */
public class GoalOptimizer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GoalOptimizer.class);
  private final List<Goal> _goalsByPriority;
  private final BalancingConstraint _balancingConstraint;
  private final Pattern _defaultExcludedTopics;
  private final LoadMonitor _loadMonitor;
  private final Time _time;
  private final int _numPrecomputingThreads;
  private final long _proposalExpirationMs;
  private final ExecutorService _proposalPrecomputingExecutor;
  private final AtomicBoolean _progressUpdateLock;
  private final AtomicReference<Exception> _proposalGenerationException;
  private final OperationProgress _proposalPrecomputingProgress;
  private final Object _cacheLock;
  private volatile OptimizerResult _cachedProposals;
  private volatile boolean _shutdown = false;
  private Thread _proposalPrecomputingSchedulerThread;
  // TODO: Make allowing/disallowing capacity estimation during proposal precomputation configurable.
  private final boolean _allowCapacityEstimationDuringProposalPrecomputing;
  private final Timer _proposalComputationTimer;
  private final ModelCompletenessRequirements _defaultModelCompletenessRequirements;
  private final ModelCompletenessRequirements _requirementsWithAvailableValidWindows;
  private final Executor _executor;
  private volatile boolean _hasOngoingExplicitPrecomputation;
  private final double _priorityWeight;
  private final double _strictnessWeight;

  /**
   * Constructor for Goal Optimizer takes the goals as input. The order of the list determines the priority of goals
   * in descending order.
   *
   * @param config The Kafka Cruise Control Configuration.
   */
  public GoalOptimizer(KafkaCruiseControlConfig config,
                       LoadMonitor loadMonitor,
                       Time time,
                       MetricRegistry dropwizardMetricRegistry,
                       Executor executor) {
    _goalsByPriority = AnalyzerUtils.getGoalsByPriority(config);
    _defaultModelCompletenessRequirements = MonitorUtils.combineLoadRequirementOptions(_goalsByPriority);
    _requirementsWithAvailableValidWindows = new ModelCompletenessRequirements(
        1,
        _defaultModelCompletenessRequirements.minMonitoredPartitionsPercentage(),
        _defaultModelCompletenessRequirements.includeAllTopics());
    _numPrecomputingThreads = config.getInt(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG);
    LOG.info("Goals by priority for precomputing: {}", _goalsByPriority);
    _balancingConstraint = new BalancingConstraint(config);
    _defaultExcludedTopics = Pattern.compile(config.getString(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG));
    _proposalExpirationMs = config.getLong(KafkaCruiseControlConfig.PROPOSAL_EXPIRATION_MS_CONFIG);
    _proposalPrecomputingExecutor =
        Executors.newScheduledThreadPool(numProposalComputingThreads(),
                                         new KafkaCruiseControlThreadFactory("ProposalPrecomputingExecutor", false, LOG));
    _loadMonitor = loadMonitor;
    _time = time;
    _cacheLock = new ReentrantLock();
    _cachedProposals = null;
    _progressUpdateLock = new AtomicBoolean(false);
    // A new AtomicReference with null initial value.
    _proposalGenerationException = new AtomicReference<>();
    _proposalPrecomputingProgress = new OperationProgress();
    _proposalComputationTimer = dropwizardMetricRegistry.timer(MetricRegistry.name("GoalOptimizer", "proposal-computation-timer"));
    _executor = executor;
    _hasOngoingExplicitPrecomputation = false;
    _priorityWeight = config.getDouble(KafkaCruiseControlConfig.GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG);
    _strictnessWeight = config.getDouble(KafkaCruiseControlConfig.GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG);
    _allowCapacityEstimationDuringProposalPrecomputing = true;
  }

  @Override
  public void run() {
    // We need to get this thread so it can be interrupted if the cached proposal has been invalidated.
    _proposalPrecomputingSchedulerThread = Thread.currentThread();
    LOG.info("Starting proposal candidate computation.");
    while (!_shutdown && _numPrecomputingThreads > 0) {
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
      long sleepTime = _proposalExpirationMs;
      if (loadMonitorTaskRunnerState == LOADING || loadMonitorTaskRunnerState == BOOTSTRAPPING) {
        LOG.info("Skipping proposal precomputing because load monitor is in " + loadMonitorTaskRunnerState + " state.");
        // Check in 30 seconds to see if the load monitor state has changed.
        sleepTime = 30000L;
      } else if (!_loadMonitor.meetCompletenessRequirements(_requirementsWithAvailableValidWindows)) {
        LOG.info("Skipping proposal precomputing because load monitor does not have enough snapshots.");
        // Check in 30 seconds to see if the load monitor has sufficient number of snapshots.
        sleepTime = 30000L;
      } else {
        try {
          if (!validCachedProposal()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Invalidated cache. Model generation (cached: {}, current: {}).{}",
                        _cachedProposals == null ? null : _cachedProposals.modelGeneration(),
                        _loadMonitor.clusterModelGeneration(),
                        _cachedProposals == null ? "" : String.format(" Cached was excluding default topics: %s.",
                                                                      _cachedProposals.excludedTopics()));
            }
            clearCachedProposal();

            long start = System.nanoTime();
            // Proposal precomputation runs with the default topics to exclude, and allows capacity estimation.
            computeCachedProposal(_allowCapacityEstimationDuringProposalPrecomputing);
            _proposalComputationTimer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
          } else {
            LOG.debug("Skipping proposal precomputing because the cached proposal result is still valid. "
                      + "Cached generation: {}", _cachedProposals.modelGeneration());
          }
        } catch (KafkaCruiseControlException e) {
          // Check in 30 seconds to see if the ongoing execution has finished.
          sleepTime = 30000L;
          LOG.debug("Skipping proposal precomputing because there is an ongoing execution.", e);
        }
      }
      long deadline = _time.milliseconds() + sleepTime;
      if (!_shutdown && _time.milliseconds() < deadline) {
        try {
          Thread.sleep(deadline - _time.milliseconds());
        } catch (InterruptedException e) {
          // let it go.
        }
      }
    }
  }

  // At least two computing thread is needed if precomputing is disabled. One thread for submitting and waiting for
  // the proposal computing to finish, another one for compute the proposals.
  private int numProposalComputingThreads() {
    return _numPrecomputingThreads > 0 ? _numPrecomputingThreads : 2;
  }

  private void computeCachedProposal(boolean allowCapacityEstimation) {
    long start = _time.milliseconds();
    Future future = _proposalPrecomputingExecutor.submit(new ProposalCandidateComputer(allowCapacityEstimation));

    try {
      boolean done = false;
      while (!_shutdown && !done) {
        try {
          future.get();
          done = true;
        } catch (InterruptedException ie) {
          LOG.debug("Goal optimizer received exception when precomputing the proposal candidates.", ie);
        }
      }
    } catch (ExecutionException ee) {
      LOG.error("Goal optimizer received exception when precomputing the proposal candidates.", ee);
    }

    LOG.info("Finished the precomputation proposal candidates in {} ms", _time.milliseconds() - start);
  }

  private boolean validCachedProposal() throws KafkaCruiseControlException {
    if (_executor.hasOngoingExecution()) {
      throw new KafkaCruiseControlException("Attempt to use proposal cache during ongoing execution.");
    }
    synchronized (_cacheLock) {
      return _cachedProposals != null && !_cachedProposals.modelGeneration().isStale(_loadMonitor.clusterModelGeneration());
    }
  }

  /**
   * Shutdown the goal optimizer.
   */
  public void shutdown() {
    LOG.info("Shutting down goal optimizer.");
    _shutdown = true;
    _proposalPrecomputingExecutor.shutdown();

    try {
      _proposalPrecomputingExecutor.awaitTermination(30000L, TimeUnit.MILLISECONDS);
      if (!_proposalPrecomputingExecutor.isTerminated()) {
        LOG.warn("The goal optimizer failed to shutdown in 30000 ms.");
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for goal optimizer to shutdown.");
    }
    LOG.info("Goal optimizer shutdown completed.");
  }

  /**
   * @return The model completeness requirements associated with the default goals.
   */
  public ModelCompletenessRequirements defaultModelCompletenessRequirements() {
    return _defaultModelCompletenessRequirements;
  }

  /**
   * @return The same model completeness requirements with {@link #_defaultModelCompletenessRequirements} except with
   * just a single available window.
   */
  public ModelCompletenessRequirements modelCompletenessRequirementsForPrecomputing() {
    return _requirementsWithAvailableValidWindows;
  }

  /**
   * @param cluster Kafka cluster.
   * @return The analyzer state from the goal optimizer.
   */
  public AnalyzerState state(Cluster cluster) {
    Map<Goal, Boolean> goalReadiness = new LinkedHashMap<>(_goalsByPriority.size());
    for (Goal goal : _goalsByPriority) {
      goalReadiness.put(goal, _loadMonitor.meetCompletenessRequirements(cluster, goal.clusterModelCompletenessRequirements()));
    }
    return new AnalyzerState(_cachedProposals != null, goalReadiness);
  }

  private void sanityCheckReadyForGettingCachedProposals() {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
    if (loadMonitorTaskRunnerState == LOADING || loadMonitorTaskRunnerState == BOOTSTRAPPING) {
      throw new IllegalStateException("Cannot get proposal because load monitor is in " + loadMonitorTaskRunnerState + " state.");
    } else if (!_loadMonitor.meetCompletenessRequirements(_requirementsWithAvailableValidWindows)) {
      throw new IllegalStateException("Cannot get proposal because model completeness is not met.");
    }
  }

  /**
   * Get the cached proposals. If the cached proposal is not valid, block waiting on the cache update.
   * We do this to avoid duplicate optimization cluster model construction and proposal computation.
   *
   * @param operationProgress to report the job progress.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return The cached proposals.
   */
  public OptimizerResult optimizations(OperationProgress operationProgress, boolean allowCapacityEstimation)
      throws InterruptedException, KafkaCruiseControlException {
    sanityCheckReadyForGettingCachedProposals();
    synchronized (_cacheLock) {
      if (!validCachedProposal() || (!allowCapacityEstimation && _cachedProposals.isCapacityEstimated())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cached proposal result is not usable. Model generation (cached: {}, current: {}). Capacity estimation"
                    + " (cached: {}, allowed: {}).{} Wait for cache update.",
                    _cachedProposals == null ? null : _cachedProposals.modelGeneration(),
                    _loadMonitor.clusterModelGeneration(),
                    _cachedProposals == null ? null : _cachedProposals.isCapacityEstimated(),
                    allowCapacityEstimation,
                    _cachedProposals == null ? "" : String.format(" Cached was excluding default topics: %s.",
                                                                  _cachedProposals.excludedTopics()));
        }
        // Invalidate the cache and set the new caching rule regarding capacity estimation.
        clearCachedProposal();
        // Explicitly allow capacity estimation to exit the loop upon computing proposals.
        while (!validCachedProposal()) {
          try {
            operationProgress.clear();
            if (_numPrecomputingThreads > 0 && (allowCapacityEstimation || !_allowCapacityEstimationDuringProposalPrecomputing)) {
              // Wake up the proposal precomputing scheduler and wait for the cache update.
              _proposalPrecomputingSchedulerThread.interrupt();
            } else if (!_hasOngoingExplicitPrecomputation) {
              // Submit background computation if there is no ongoing explicit precomputation and wait for the cache update.
              _hasOngoingExplicitPrecomputation = true;
              _proposalPrecomputingExecutor.submit(() -> computeCachedProposal(allowCapacityEstimation));
            }
            operationProgress.refer(_proposalPrecomputingProgress);
            _cacheLock.wait();
          } finally {
            Exception proposalGenerationException = _proposalGenerationException.get();
            if (proposalGenerationException != null) {
              // There has been an exception during the cached proposal creation -- throw this exception to prevent unbounded wait.
              LOG.error("Cannot create cached proposals due to exception.", proposalGenerationException);
              throw new KafkaCruiseControlException(proposalGenerationException);
            }
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Returning optimization result from cache. Model generation (cached: {}, current: {}).",
                  _cachedProposals.modelGeneration(), _loadMonitor.clusterModelGeneration());
      }

      return _cachedProposals;
    }
  }

  /**
   * Depending the existence of dead/decommissioned brokers in the given cluster:
   * (1) Re-balance: Generates proposals to update the state of the cluster to achieve a final balanced state.
   * (2) Self-healing: Generates proposals to move replicas away from decommissioned brokers.
   *
   * @param clusterModel The state of the cluster over which the balancing proposal will be applied. Function execution
   *                     updates the cluster state with balancing proposals. If the cluster model is specified, the
   *                     cached proposal will be ignored.
   * @param operationProgress to report the job progress.
   * @return Results of optimization containing the proposals and stats.
   */
  public OptimizerResult optimizations(ClusterModel clusterModel, OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    return optimizations(clusterModel, _goalsByPriority, operationProgress);
  }

  /**
   * Provides optimization
   * <ul>
   *   <li>using {@link #_defaultExcludedTopics}.</li>
   *   <li>does not exclude any brokers for receiving leadership.</li>
   *   <li>does not exclude any brokers for receiving replicas.</li>
   *   <li>assumes that the optimization is not triggered by anomaly detector.</li>
   *   <li>does not specify the destination brokers for replica move explicitly.</li>
   *   <li>does not keep the movements limited to immigrant replicas.</li>
   * </ul>
   *
   * See {@link GoalOptimizer#optimizations(ClusterModel, List, OperationProgress, Map, OptimizationOptions)}.
   * @return Results of optimization containing the proposals and stats.
   */
  public OptimizerResult optimizations(ClusterModel clusterModel,
                                       List<Goal> goalsByPriority,
                                       OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    if (clusterModel == null) {
      throw new IllegalArgumentException("The cluster model cannot be null");
    } else if (goalsByPriority.isEmpty()) {
      throw new IllegalArgumentException("At least one goal must be provided to get an optimization result.");
    } else if (!clusterModel.isClusterAlive()) {
      throw new IllegalArgumentException("All brokers are dead in the cluster.");
    }

    Set<String> excludedTopics = excludedTopics(clusterModel, null);
    LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
    OptimizationOptions optimizationOptions = new OptimizationOptions(excludedTopics,
                                                                      Collections.emptySet(),
                                                                      Collections.emptySet(),
                                                                      false,
                                                                      Collections.emptySet(),
                                                                      false);
    return optimizations(clusterModel, goalsByPriority, operationProgress, null, optimizationOptions);
  }

  /**
   * Depending the existence of dead/broken/decommissioned brokers in the given cluster:
   * (1) Re-balance: Generates proposals to update the state of the cluster to achieve a final balanced state.
   * (2) Self-healing: Generates proposals to move replicas away from decommissioned brokers and broken disks.
   * Returns a map from goal names to stats. Initial stats are returned under goal name "init".
   *
   * Assumptions:
   * <ul>
   *   <li>The cluster model cannot be null.</li>
   *   <li>At least one goal has been provided in goalsByPriority.</li>
   *   <li>There is at least one alive broker in the cluster.</li>
   * </ul>
   *
   * @param clusterModel The state of the cluster over which the balancing proposal will be applied. Function execution
   *                     updates the cluster state with balancing proposals. If the cluster model is specified, the
   *                     cached proposal will be ignored.
   * @param goalsByPriority the goals ordered by priority.
   * @param operationProgress to report the job progress.
   * @param initReplicaDistributionForProposalGeneration The initial replica distribution of the cluster. This is only
   *                                                     needed if the passed in clusterModel is not the original cluster
   *                                                     model so that initial replica distribution can not be deducted
   *                                                     from that cluster model, otherwise it is null. One case explicitly
   *                                                     specifying initial replica distribution needed is to increase/decrease
   *                                                     specific topic partition's replication factor, in this case some
   *                                                     replicas are tentatively deleted/added in cluster model before
   *                                                     passing it in to generate proposals.
   * @return Results of optimization containing the proposals and stats.
   */
  public OptimizerResult optimizations(ClusterModel clusterModel,
                                       List<Goal> goalsByPriority,
                                       OperationProgress operationProgress,
                                       Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistributionForProposalGeneration,
                                       OptimizationOptions optimizationOptions)
      throws KafkaCruiseControlException {
    LOG.trace("Cluster before optimization is {}", clusterModel);
    BrokerStats brokerStatsBeforeOptimization = clusterModel.brokerStats(null);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();
    boolean isSelfHealing = !clusterModel.selfHealingEligibleReplicas().isEmpty();

    // Set of balancing proposals that will be applied to the given cluster state to satisfy goals (leadership
    // transfer AFTER partition transfer.)
    Set<Goal> optimizedGoals = new HashSet<>(goalsByPriority.size());
    Set<String> violatedGoalNamesBeforeOptimization = new HashSet<>();
    Set<String> violatedGoalNamesAfterOptimization = new HashSet<>();
    LinkedHashMap<Goal, ClusterModelStats> statsByGoalPriority = new LinkedHashMap<>(goalsByPriority.size());
    Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedReplicaDistribution = null;
    Map<TopicPartition, ReplicaPlacementInfo> preOptimizedLeaderDistribution = null;

    for (Goal goal : goalsByPriority) {
      preOptimizedReplicaDistribution = preOptimizedReplicaDistribution == null ? initReplicaDistribution : clusterModel.getReplicaDistribution();
      preOptimizedLeaderDistribution = preOptimizedLeaderDistribution == null ? initLeaderDistribution : clusterModel.getLeaderDistribution();
      OptimizationForGoal step = new OptimizationForGoal(goal.name());
      operationProgress.addStep(step);
      LOG.debug("Optimizing goal {}", goal.name());
      boolean succeeded = goal.optimize(clusterModel, optimizedGoals, optimizationOptions);
      optimizedGoals.add(goal);
      statsByGoalPriority.put(goal, clusterModel.getClusterStats(_balancingConstraint));

      Set<ExecutionProposal> goalProposals = AnalyzerUtils.getDiff(preOptimizedReplicaDistribution,
                                                                   preOptimizedLeaderDistribution,
                                                                   clusterModel);
      if (!goalProposals.isEmpty() || !succeeded) {
        violatedGoalNamesBeforeOptimization.add(goal.name());
      }
      if (!succeeded) {
        violatedGoalNamesAfterOptimization.add(goal.name());
      }
      logProgress(isSelfHealing, goal.name(), optimizedGoals.size(), goalProposals);
      step.done();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Broker level stats after optimization: {}", clusterModel.brokerStats(null));
      }
    }

    // Broker level stats in the final cluster state.
    if (LOG.isTraceEnabled()) {
      LOG.trace("Broker level stats after optimization: {}%n", clusterModel.brokerStats(null));
    }

    // Skip replication factor change check here since in above iteration we already check for each goal it does not change
    // any partition's replication factor.
    Set<ExecutionProposal> proposals =
        AnalyzerUtils.getDiff(initReplicaDistributionForProposalGeneration != null ? initReplicaDistributionForProposalGeneration
                                                                                   : initReplicaDistribution,
                              initLeaderDistribution,
                              clusterModel,
                              true);
    return new OptimizerResult(statsByGoalPriority,
                               violatedGoalNamesBeforeOptimization,
                               violatedGoalNamesAfterOptimization,
                               proposals,
                               brokerStatsBeforeOptimization,
                               clusterModel.brokerStats(null),
                               clusterModel.generation(),
                               clusterModel.getClusterStats(_balancingConstraint),
                               clusterModel.capacityEstimationInfoByBrokerId(),
                               optimizationOptions,
                               balancednessCostByGoal(goalsByPriority, _priorityWeight, _strictnessWeight));
  }

  /**
   * Get set of excluded topics in the given cluster model.
   *
   * @param clusterModel The state of the cluster.
   * @param requestedExcludedTopics Pattern used to exclude topics, or if {@code null}, use {@link #_defaultExcludedTopics}.
   * @return Set of excluded topics in the given cluster model.
   */
  public Set<String> excludedTopics(ClusterModel clusterModel, Pattern requestedExcludedTopics) {
    Pattern topicsToExclude = requestedExcludedTopics != null ? requestedExcludedTopics : _defaultExcludedTopics;
    return clusterModel.topics()
                       .stream()
                       .filter(topic -> topicsToExclude.matcher(topic).matches())
                       .collect(Collectors.toSet());
  }

  /**
   * Log the progress of goal optimizer.
   *
   * @param isSelfHeal     True if self healing, false otherwise.
   * @param goalName       Goal name.
   * @param proposals      Goal proposals.
   */
  private void logProgress(boolean isSelfHeal,
                           String goalName,
                           int numOptimizedGoals,
                           Set<ExecutionProposal> proposals) {
    LOG.debug("[{}/{}] Generated {} proposals for {}{}.", numOptimizedGoals, _goalsByPriority.size(), proposals.size(),
              isSelfHeal ? "self-healing " : "", goalName);
    LOG.trace("Proposals for {}{}.{}%n", isSelfHeal ? "self-healing " : "", goalName, proposals);
  }

  private OptimizerResult updateCachedProposals(OptimizerResult result) {
    synchronized (_cacheLock) {
      _hasOngoingExplicitPrecomputation = false;
      _cachedProposals = result;
      // Wake up any thread that is waiting for a proposal update.
      _cacheLock.notifyAll();
      return _cachedProposals;
    }
  }

  private void clearCachedProposal(Exception e) {
    synchronized (_cacheLock) {
      _cachedProposals = null;
      _progressUpdateLock.set(false);
      _proposalPrecomputingProgress.clear();
      _proposalGenerationException.set(e);
    }
  }

  private void clearCachedProposal() {
    clearCachedProposal(null);
  }

  /**
   * A class that precomputes the proposal candidates and find the cached proposals.
   */
  private class ProposalCandidateComputer implements Runnable {
    private final boolean _allowCapacityEstimation;
    ProposalCandidateComputer(boolean allowCapacityEstimation) {
      _allowCapacityEstimation = allowCapacityEstimation;
    }

    @Override
    public void run() {
      LOG.debug("Starting proposal candidate computer.");
      if (_loadMonitor == null) {
        LOG.warn("No load monitor available. Skip computing proposal candidate.");
        return;
      }
      OperationProgress operationProgress =
          _progressUpdateLock.compareAndSet(false, true) ? _proposalPrecomputingProgress : new OperationProgress();

      try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
        long startMs = _time.milliseconds();
        // We compute the proposal even if there is not enough modeled partitions.
        ModelCompletenessRequirements requirements = _loadMonitor.meetCompletenessRequirements(_defaultModelCompletenessRequirements) ?
                                                     _defaultModelCompletenessRequirements : _requirementsWithAvailableValidWindows;
        ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(), requirements, operationProgress);
        sanityCheckCapacityEstimation(_allowCapacityEstimation, clusterModel.capacityEstimationInfoByBrokerId());
        if (!clusterModel.topics().isEmpty()) {
          OptimizerResult result = optimizations(clusterModel, _goalsByPriority, operationProgress);
          LOG.debug("Generated a proposal candidate in {} ms.", _time.milliseconds() - startMs);
          updateCachedProposals(result);
        } else {
          LOG.warn("The cluster model does not have valid topics, skipping proposal precomputation.");
        }
      } catch (OptimizationFailureException ofe) {
        LOG.warn("Detected unfixable proposal optimization", ofe);
        exceptionHandler(ofe);
      } catch (Exception e) {
        LOG.error("Proposal precomputation encountered error", e);
        exceptionHandler(e);
      }
    }

    private void exceptionHandler(Exception e) {
      clearCachedProposal(e);
      synchronized (_cacheLock) {
        _hasOngoingExplicitPrecomputation = false;
        // Wake up any thread that is waiting for a proposal update.
        _cacheLock.notifyAll();
      }
    }

  }
}
