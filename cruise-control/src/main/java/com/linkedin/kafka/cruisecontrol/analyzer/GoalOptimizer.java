/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.async.progress.OptimizationForGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING;
import static com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING;


/**
 * A class for optimizing goals in the given order of priority.
 */
public class GoalOptimizer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GoalOptimizer.class);
  private final SortedMap<Integer, Goal> _goalsByPriority;
  private final BalancingConstraint _balancingConstraint;
  private final Pattern _defaultExcludedTopics;
  private final LoadMonitor _loadMonitor;

  private final Time _time;
  private final int _numPrecomputingThreads;
  private final long _proposalExpirationMs;
  private final ExecutorService _proposalPrecomputingExecutor;
  private final AtomicBoolean _progressUpdateLock;
  private final OperationProgress _proposalPrecomputingProgress;
  private final List<SortedMap<Integer, Goal>> _goalByPriorityForPrecomputing;
  private final Object _cacheLock;
  private final AtomicInteger _threadsWaitingForCache;
  private volatile OptimizerResult _bestProposal;
  private volatile boolean _shutdown = false;
  private Thread _proposalPrecomputingSchedulerThread;
  private final Timer _proposalComputationTimer;
  private final ModelCompletenessRequirements _defaultModelCompletenessRequirements;
  private final ModelCompletenessRequirements _requirementsWithAvailableValidWindows;

  /**
   * Constructor for Goal Optimizer takes the goals as input. The order of the list determines the priority of goals
   * in descending order.
   *
   * @param config The Kafka Cruise Control Configuration.
   */
  public GoalOptimizer(KafkaCruiseControlConfig config,
                       LoadMonitor loadMonitor,
                       Time time,
                       MetricRegistry dropwizardMetricRegistry) {
    _goalsByPriority = AnalyzerUtils.getGoalMapByPriority(config);
    _defaultModelCompletenessRequirements = MonitorUtils.combineLoadRequirementOptions(_goalsByPriority.values());
    _requirementsWithAvailableValidWindows = new ModelCompletenessRequirements(
        1,
        _defaultModelCompletenessRequirements.minMonitoredPartitionsPercentage(),
        _defaultModelCompletenessRequirements.includeAllTopics());
    _goalByPriorityForPrecomputing = new ArrayList<>();
    // The number of proposal precomputing thread should not exceed the number of unique goal priority combinations.
    _numPrecomputingThreads = Math.min(config.getInt(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG),
                                       AnalyzerUtils.factorial(_goalsByPriority.size()));
    // Generate different goal priorities for each of proposal precomputing thread.
    populateGoalByPriorityForPrecomputing();
    LOG.info("Goals by priority: {}", _goalsByPriority);
    LOG.info("Goals by priority for proposal precomputing: {}", _goalByPriorityForPrecomputing);
    _balancingConstraint = new BalancingConstraint(config);
    _defaultExcludedTopics = Pattern.compile(config.getString(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG));
    _proposalExpirationMs = config.getLong(KafkaCruiseControlConfig.PROPOSAL_EXPIRATION_MS_CONFIG);
    _proposalPrecomputingExecutor =
        Executors.newScheduledThreadPool(numProposalComputingThreads(),
                                         new KafkaCruiseControlThreadFactory("ProposalPrecomputingExecutor", false, LOG));
    _loadMonitor = loadMonitor;
    _time = time;
    _cacheLock = new ReentrantLock();
    _threadsWaitingForCache = new AtomicInteger(0);
    _bestProposal = null;
    _progressUpdateLock = new AtomicBoolean(false);
    _proposalPrecomputingProgress = new OperationProgress();
    _proposalComputationTimer = dropwizardMetricRegistry.timer(MetricRegistry.name("GoalOptimizer", "proposal-computation-timer"));
  }

  private void populateGoalByPriorityForPrecomputing() {
    Set<List<Goal>> shuffledGoals = new HashSet<>();

    // Calculate the number of goals to compute the permutations.
    int numberOfGoalsToComputePermutations = 0;
    int numShuffledGoalsToGenerate = _numPrecomputingThreads > 0 ? _numPrecomputingThreads : 1;
    while (true) {
      if (AnalyzerUtils.factorial(++numberOfGoalsToComputePermutations) >= numShuffledGoalsToGenerate) {
        break;
      }
    }

    // Get all permutations of the last numberOfGoalsToComputePermutations goals.
    int toIndex = _goalsByPriority.size() - numberOfGoalsToComputePermutations;
    List<Goal> commonPrefix = new ArrayList<>(_goalsByPriority.values()).subList(0, toIndex);
    List<Goal> suffixToPermute = new ArrayList<>(_goalsByPriority.values()).subList(toIndex, _goalsByPriority.size());

    for (List<Goal> shuffledSuffix: AnalyzerUtils.getPermutations(suffixToPermute)) {
      List<Goal> shuffledGoalList = new ArrayList<>(commonPrefix);
      shuffledGoalList.addAll(shuffledSuffix);
      shuffledGoals.add(shuffledGoalList);
    }

    // Guarantee that one thread is working on the original goal priority.
    addGoalsForPrecomputing(new ArrayList<>(_goalsByPriority.values()));
    // Add the remaining goal priorities.
    addShuffledGoalsForPrecomputing(shuffledGoals);
  }

  private void addShuffledGoalsForPrecomputing(Set<List<Goal>> shuffledGoals) {
    List<Goal> originalGoals = new ArrayList<>(_goalsByPriority.values());

    // Add the others
    int numShuffledGoalsToGenerate = _numPrecomputingThreads > 0 ? _numPrecomputingThreads : 1;
    for (List<Goal> shuffledGoalList : shuffledGoals) {
      if (_goalByPriorityForPrecomputing.size() == numShuffledGoalsToGenerate) {
        break;
      }
      boolean isOriginalOrder = true;
      for (int i = 0; i < shuffledGoalList.size(); i++) {
        String shuffledGoal = shuffledGoalList.get(i).name();
        if (!shuffledGoal.equals(originalGoals.get(i).name())) {
          isOriginalOrder = false;
          break;
        }
      }
      if (!isOriginalOrder) {
        addGoalsForPrecomputing(shuffledGoalList);
      }
    }
  }

  private void addGoalsForPrecomputing(List<Goal> goals) {
    SortedMap<Integer, Goal> shuffledGoalByPriority = new TreeMap<>();
    int j = 0;
    for (Goal goal : goals) {
      shuffledGoalByPriority.put(j++, goal);
    }
    _goalByPriorityForPrecomputing.add(shuffledGoalByPriority);
  }

  /**
   * Package private for unit test.
   */
  List<SortedMap<Integer, Goal>> goalByPriorityForPrecomputing() {
    return _goalByPriorityForPrecomputing;
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
        LOG.info("Skipping best proposal precomputing because load monitor is in " + loadMonitorTaskRunnerState + " state.");
        // Check in 30 seconds to see if the load monitor state has changed.
        sleepTime = 30000L;
      } else if (!_loadMonitor.meetCompletenessRequirements(_requirementsWithAvailableValidWindows)) {
        LOG.info("Skipping best proposal precomputing because load monitor does not have enough snapshots.");
        // Check in 30 seconds to see if the load monitor has sufficient number of snapshots.
        sleepTime = 30000L;
      } else if (!validCachedProposal()) {
        LOG.debug("Invalidated cache. Model generation (cached: {}, current: {}).{}",
                  _bestProposal == null ? null : _bestProposal.modelGeneration(),
                  _loadMonitor.clusterModelGeneration(),
                  _bestProposal == null ? "" : String.format(" Cached was excluding default topics: %s.",
                                                             _bestProposal.excludedTopics()));
        clearBestProposal();

        long start = System.nanoTime();
        // Proposal precomputation runs with the default topics to exclude.
        computeBestProposal();
        _proposalComputationTimer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      } else {
        LOG.debug("Skipping best proposal precomputing because the cached best result is still valid. "
                      + "Cached generation: {}", _bestProposal.modelGeneration());
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

  private void computeBestProposal() {
    long start = _time.milliseconds();
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < (_numPrecomputingThreads > 0 ? _numPrecomputingThreads : 1); i++) {
      futures.add(_proposalPrecomputingExecutor.submit(
          new ProposalCandidateComputer(_goalByPriorityForPrecomputing.get(i))));
    }
    for (Future future : futures) {
      try {
        boolean done = false;
        while (!_shutdown && !done) {
          try {
            future.get();
            done = true;
          } catch (InterruptedException ie) {
            LOG.debug("Goal optimizer received exception when precomputing the proposal candidates {}.", ie.toString());
          }
        }
      } catch (ExecutionException ee) {
        LOG.error("Goal optimizer received exception when precomputing the proposal candidates.", ee);
      }
    }
    if (!futures.isEmpty()) {
      LOG.info("Finished the precomputation proposal candidates in {} ms", _time.milliseconds() - start);
    }
  }

  private boolean validCachedProposal() {
    synchronized (_cacheLock) {
      return _bestProposal != null
             && _bestProposal.modelGeneration().equals(_loadMonitor.clusterModelGeneration());
    }
  }

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

  public ModelCompletenessRequirements defaultModelCompletenessRequirements() {
    return _defaultModelCompletenessRequirements;
  }

  public ModelCompletenessRequirements modelCompletenessRequirementsForPrecomputing() {
    return _requirementsWithAvailableValidWindows;
  }

  /**
   * Get the analyzer state from the goal optimizer.
   */
  public AnalyzerState state(MetadataClient.ClusterAndGeneration clusterAndGeneration) {
    Map<Goal, Boolean> goalReadiness = new LinkedHashMap<>(_goalsByPriority.size());
    for (Goal goal : _goalsByPriority.values()) {
      goalReadiness.put(goal, _loadMonitor.meetCompletenessRequirements(clusterAndGeneration,
                                                                        goal.clusterModelCompletenessRequirements()));
    }
    return new AnalyzerState(_bestProposal != null, goalReadiness);
  }

  /**
   * Get the cached proposals. If the cached proposal is not valid, block waiting on the cache update.
   * We do this to avoid duplicate optimization cluster model construction and proposal computation.
   *
   * @param operationProgress to report the job progress.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   */
  public OptimizerResult optimizations(OperationProgress operationProgress,
                                       boolean allowCapacityEstimation) throws InterruptedException {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
    if (loadMonitorTaskRunnerState == LOADING || loadMonitorTaskRunnerState == BOOTSTRAPPING) {
      throw new IllegalStateException("Cannot get proposal because load monitor is in " + loadMonitorTaskRunnerState + " state.");
    } else if (!_loadMonitor.meetCompletenessRequirements(_requirementsWithAvailableValidWindows)) {
      throw new IllegalStateException("Cannot get proposal because model completeness is not met.");
    }
    synchronized (_cacheLock) {
      if (!validCachedProposal() || (!allowCapacityEstimation && _bestProposal.isCapacityEstimated())) {
        LOG.debug("Cached best proposal is not usable. Model generation (cached: {}, current: {}). Capacity estimation"
                  + " (cached: {}, allowed: {}).{} Wait for cache update.",
                  _bestProposal == null ? null : _bestProposal.modelGeneration(),
                  _loadMonitor.clusterModelGeneration(),
                  _bestProposal == null ? null : _bestProposal.isCapacityEstimated(),
                  allowCapacityEstimation,
                  _bestProposal == null ? "" : String.format(" Cached was excluding default topics: %s.",
                                                             _bestProposal.excludedTopics()));
        // Invalidate the cache and set the new caching rule regarding capacity estimation.
        _bestProposal = null;
        // Explicitly allow capacity estimation to exit the loop upon computing best proposals.
        while (!validCachedProposal()) {
          try {
            // Prevent multiple thread submit from computing task together.
            int numWaitingThread = _threadsWaitingForCache.getAndIncrement();
            if (_numPrecomputingThreads > 0) {
              // Wake up the proposal precomputing scheduler and wait for the cache update.
              _proposalPrecomputingSchedulerThread.interrupt();
              operationProgress.refer(_proposalPrecomputingProgress);
            } else {
              // Only submit task if nobody has submitted the computing task.
              if (numWaitingThread == 0) {
                // No precomputing thread is available, schedule a computing and wait for the cache update.
                _proposalPrecomputingExecutor.submit(this::computeBestProposal);
              }
            }
            _cacheLock.wait();
          } finally {
            _threadsWaitingForCache.decrementAndGet();
          }
        }
      }
      LOG.debug("Returning optimization result from cache. Cached generation: {}", _bestProposal.modelGeneration());
      // If the cached proposals are estimated, and the user requested for no estimation, we invalidate the cache and
      // calculate best proposals just once. If the returned result still involves an estimation, throw an exception.
      KafkaCruiseControl.sanityCheckCapacityEstimation(allowCapacityEstimation, _bestProposal.capacityEstimationInfoByBrokerId());
      return _bestProposal;
    }
  }

  /**
   * Depending the existence of dead/decommissioned brokers in the given cluster:
   * (1) Re-balance: Generates proposals to update the state of the cluster to achieve a final balanced state.
   * (2) Self-healing: Generates proposals to move replicas away from decommissioned brokers.
   * Returns a map from goal names to stats. Initial stats are returned under goal name "init".
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
   * Provides optimization using {@link #_defaultExcludedTopics}.
   *
   * See {@link #optimizations(ClusterModel, Map, OperationProgress, Pattern)}.
   */
  public OptimizerResult optimizations(ClusterModel clusterModel,
                                       Map<Integer, Goal> goalsByPriority,
                                       OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    return optimizations(clusterModel, goalsByPriority, operationProgress, null);
  }

  /**
   * Depending the existence of dead/decommissioned brokers in the given cluster:
   * (1) Re-balance: Generates proposals to update the state of the cluster to achieve a final balanced state.
   * (2) Self-healing: Generates proposals to move replicas away from decommissioned brokers.
   * Returns a map from goal names to stats. Initial stats are returned under goal name "init".
   *
   * @param clusterModel The state of the cluster over which the balancing proposal will be applied. Function execution
   *                     updates the cluster state with balancing proposals. If the cluster model is specified, the
   *                     cached proposal will be ignored.
   * @param goalsByPriority the goals ordered by priority.
   * @param operationProgress to report the job progress.
   * @param requestedExcludedTopics Topics excluded from partition movement (if null, use {@link #_defaultExcludedTopics})
   * @return Results of optimization containing the proposals and stats.
   */
  public OptimizerResult optimizations(ClusterModel clusterModel,
                                       Map<Integer, Goal> goalsByPriority,
                                       OperationProgress operationProgress,
                                       Pattern requestedExcludedTopics)
      throws KafkaCruiseControlException {
    if (clusterModel == null) {
      throw new IllegalArgumentException("The cluster model cannot be null");
    }

    // Sanity check for optimizing goals.
    if (!clusterModel.isClusterAlive()) {
      throw new IllegalArgumentException("All brokers are dead in the cluster.");
    }

    LOG.trace("Cluster before optimization is {}", clusterModel);
    ClusterModel.BrokerStats brokerStatsBeforeOptimization = clusterModel.brokerStats();
    Map<TopicPartition, List<Integer>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, Integer> initLeaderDistribution = clusterModel.getLeaderDistribution();
    boolean isSelfHealing = !clusterModel.selfHealingEligibleReplicas().isEmpty();

    // Set of balancing proposals that will be applied to the given cluster state to satisfy goals (leadership
    // transfer AFTER partition transfer.)
    Set<Goal> optimizedGoals = new HashSet<>();
    Set<Goal> violatedGoalsBeforeOptimization = new HashSet<>();
    Set<Goal> violatedGoalsAfterOptimization = new HashSet<>();
    Map<Goal, ClusterModelStats> statsByGoalPriority = new LinkedHashMap<>();
    Map<TopicPartition, List<Integer>> preOptimizedReplicaDistribution = null;
    Map<TopicPartition, Integer> preOptimizedLeaderDistribution = null;
    Set<String> excludedTopics = excludedTopics(clusterModel, requestedExcludedTopics);
    LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
    for (Map.Entry<Integer, Goal> entry : goalsByPriority.entrySet()) {
      preOptimizedReplicaDistribution = preOptimizedReplicaDistribution == null ? initReplicaDistribution : clusterModel.getReplicaDistribution();
      preOptimizedLeaderDistribution = preOptimizedLeaderDistribution == null ? initLeaderDistribution : clusterModel.getLeaderDistribution();
      Goal goal = entry.getValue();
      OptimizationForGoal step = new OptimizationForGoal(goal.name());
      operationProgress.addStep(step);
      LOG.debug("Optimizing goal {}", goal.name());
      boolean succeeded = goal.optimize(clusterModel, optimizedGoals, excludedTopics);
      optimizedGoals.add(goal);
      statsByGoalPriority.put(goal, clusterModel.getClusterStats(_balancingConstraint));

      Set<ExecutionProposal> goalProposals = AnalyzerUtils.getDiff(preOptimizedReplicaDistribution,
                                                                   preOptimizedLeaderDistribution,
                                                                   clusterModel);
      if (!goalProposals.isEmpty() || !succeeded) {
        violatedGoalsBeforeOptimization.add(goal);
      }
      if (!succeeded) {
        violatedGoalsAfterOptimization.add(goal);
      }
      logProgress(isSelfHealing, goal.name(), optimizedGoals.size(), goalProposals);
      step.done();
      LOG.debug("Broker level stats after optimization: {}", clusterModel.brokerStats());
    }

    clusterModel.sanityCheck();
    // Broker level stats in the final cluster state.
    if (LOG.isTraceEnabled()) {
      LOG.trace("Broker level stats after optimization: {}%n", clusterModel.brokerStats());
    }

    Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    return new OptimizerResult(statsByGoalPriority,
                               violatedGoalsBeforeOptimization,
                               violatedGoalsAfterOptimization,
                               proposals,
                               brokerStatsBeforeOptimization,
                               clusterModel.brokerStats(),
                               clusterModel.generation(),
                               clusterModel.getClusterStats(_balancingConstraint),
                               clusterModel.capacityEstimationInfoByBrokerId(),
                               excludedTopics);
  }

  private Set<String> excludedTopics(ClusterModel clusterModel, Pattern requestedExcludedTopics) {
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

  private OptimizerResult updateBestProposal(OptimizerResult result) {
    synchronized (_cacheLock) {
      if (!validCachedProposal()) {
        LOG.debug("Updated best proposal, broker stats: \n{}",
                  result.brokerStatsAfterOptimization());
        _bestProposal = result;
      } else {
        boolean shouldUpdate = true;
        for (Goal goal: _goalsByPriority.values()) {
          shouldUpdate = shouldUpdate
                         && goal.clusterModelStatsComparator().compare(result.clusterModelStats(), _bestProposal
              .clusterModelStats()) >= 0;
        }

        if (shouldUpdate) {
          LOG.debug("Updated best proposal, broker stats: \n{}", result.brokerStatsAfterOptimization());
          _bestProposal = result;
        }
      }
      // Wake up any thread that is waiting for a proposal update.
      _cacheLock.notifyAll();
      return _bestProposal;
    }
  }

  private void clearBestProposal() {
    synchronized (_cacheLock) {
      _bestProposal = null;
      _progressUpdateLock.set(false);
      _proposalPrecomputingProgress.clear();
    }
  }

  /**
   * A class for representing the results of goal optimizer. The results include stats by goal priority and
   * optimization proposals.
   */
  public static class OptimizerResult {
    private final Map<Goal, ClusterModelStats> _statsByGoalPriority;
    private final Set<ExecutionProposal> _optimizationProposals;
    private final Set<Goal> _violatedGoalsBeforeOptimization;
    private final Set<Goal> _violatedGoalsAfterOptimization;
    private final ClusterModel.BrokerStats _brokerStatsBeforeOptimization;
    private final ClusterModel.BrokerStats _brokerStatsAfterOptimization;
    private final ModelGeneration _modelGeneration;
    private final ClusterModelStats _clusterModelStats;
    private final Map<Integer, String> _capacityEstimationInfoByBrokerId;
    private final Set<String> _excludedTopics;

    OptimizerResult(Map<Goal, ClusterModelStats> statsByGoalPriority,
                    Set<Goal> violatedGoalsBeforeOptimization,
                    Set<Goal> violatedGoalsAfterOptimization,
                    Set<ExecutionProposal> optimizationProposals,
                    ClusterModel.BrokerStats brokerStatsBeforeOptimization,
                    ClusterModel.BrokerStats brokerStatsAfterOptimization,
                    ModelGeneration modelGeneration,
                    ClusterModelStats clusterModelStats,
                    Map<Integer, String> capacityEstimationInfoByBrokerId,
                    Set<String> excludedTopics) {
      _statsByGoalPriority = statsByGoalPriority;
      _violatedGoalsBeforeOptimization = violatedGoalsBeforeOptimization;
      _violatedGoalsAfterOptimization = violatedGoalsAfterOptimization;
      _optimizationProposals = optimizationProposals;
      _brokerStatsBeforeOptimization = brokerStatsBeforeOptimization;
      _brokerStatsAfterOptimization = brokerStatsAfterOptimization;
      _modelGeneration = modelGeneration;
      _clusterModelStats = clusterModelStats;
      _capacityEstimationInfoByBrokerId = capacityEstimationInfoByBrokerId;
      _excludedTopics = excludedTopics;
    }

    public Map<Goal, ClusterModelStats> statsByGoalPriority() {
      return _statsByGoalPriority;
    }

    public Set<ExecutionProposal> goalProposals() {
      return _optimizationProposals;
    }

    public Set<Goal> violatedGoalsBeforeOptimization() {
      return _violatedGoalsBeforeOptimization;
    }

    public Set<Goal> violatedGoalsAfterOptimization() {
      return _violatedGoalsAfterOptimization;
    }

    public ModelGeneration modelGeneration() {
      return _modelGeneration;
    }

    public ClusterModelStats clusterModelStats() {
      return _clusterModelStats;
    }

    public ClusterModel.BrokerStats brokerStatsBeforeOptimization() {
      return _brokerStatsBeforeOptimization;
    }

    public ClusterModel.BrokerStats brokerStatsAfterOptimization() {
      return _brokerStatsAfterOptimization;
    }

    public boolean isCapacityEstimated() {
      return !_capacityEstimationInfoByBrokerId.isEmpty();
    }

    public Map<Integer, String> capacityEstimationInfoByBrokerId() {
      return Collections.unmodifiableMap(_capacityEstimationInfoByBrokerId);
    }

    public Set<String> excludedTopics() {
      return _excludedTopics;
    }

    private List<Number> getMovementStats() {
      Integer numReplicaMovements = 0;
      Integer numLeaderMovements = 0;
      long dataToMove = 0L;
      for (ExecutionProposal p : _optimizationProposals) {
        if (!p.replicasToAdd().isEmpty() || !p.replicasToRemove().isEmpty()) {
          numReplicaMovements++;
          dataToMove += p.dataToMoveInMB();
        } else {
          numLeaderMovements++;
        }
      }
      return Arrays.asList(numReplicaMovements, dataToMove, numLeaderMovements);
    }

    public String getProposalSummary() {
      List<Number> moveStats = getMovementStats();
      return String.format("%n%nThe optimization proposal has %d replica(%d MB) movements and %d leadership movements "
                           + "based on the cluster model with %d recent snapshot windows and %.3f%% of the partitions "
                           + "covered.%nExcluded Topics: %s",
                           moveStats.get(0).intValue(), moveStats.get(1).longValue(), moveStats.get(2).intValue(),
                           _clusterModelStats.numSnapshotWindows(), _clusterModelStats.monitoredPartitionsPercentage() * 100,
                           _excludedTopics);
    }

    public Map<String, Object> getProposalSummaryForJson() {
      List<Number> moveStats = getMovementStats();
      Map<String, Object> ret = new HashMap<>();
      ret.put("numReplicaMovements", moveStats.get(0).intValue());
      ret.put("dataToMoveMB", moveStats.get(1).longValue());
      ret.put("numLeaderMovements", moveStats.get(2).intValue());
      ret.put("recentWindows", _clusterModelStats.numSnapshotWindows());
      ret.put("monitoredPartitionsPercentage", _clusterModelStats.monitoredPartitionsPercentage() * 100.0);
      ret.put("excludedTopics", _excludedTopics);
      return ret;
    }
  }

  /**
   * A class that precomputes the proposal candidates and find the best proposal.
   */
  private class ProposalCandidateComputer implements Runnable {
    private final TreeMap<Integer, Goal> _goalByPriority;

    ProposalCandidateComputer(Map<Integer, Goal> goalByPriority) {
      _goalByPriority = new TreeMap<>(goalByPriority);
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
        if (!clusterModel.topics().isEmpty()) {
          OptimizerResult result = optimizations(clusterModel, _goalByPriority, operationProgress);
          LOG.debug("Generated a proposal candidate in {} ms.", _time.milliseconds() - startMs);
          updateBestProposal(result);
        } else {
          LOG.warn("The cluster model does not have valid topics, skipping proposal precomputation.");
        }
      } catch (Exception e) {
        LOG.error("Proposal precomputation encountered error", e);
      }
    }
  }
}
