/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.exception.AnalysisInputException;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.ArrayList;
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
  private final Pattern _excludedTopics;
  private final LoadMonitor _loadMonitor;

  private final Time _time;
  private final int _maxProposalCandidates;
  private final int _numPrecomputingThreads;
  private final long _proposalExpirationMs;
  private final ExecutorService _proposalPrecomputingExecutor;
  private final AtomicInteger _totalProposalCandidateComputed;
  private final List<SortedMap<Integer, Goal>> _goalByPriorityForPrecomputing;
  private final Object _cacheLock;
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
    _numPrecomputingThreads = config.getInt(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG);
    for (int i = 0; i < _numPrecomputingThreads; i++) {
      _goalByPriorityForPrecomputing.add(AnalyzerUtils.getGoalMapByPriority(config));
    }
    LOG.info("{}", _goalsByPriority);
    LOG.info("{}", _goalByPriorityForPrecomputing);
    _balancingConstraint = new BalancingConstraint(config);
    _excludedTopics = Pattern.compile(config.getString(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG));
    _maxProposalCandidates = config.getInt(KafkaCruiseControlConfig.MAX_PROPOSAL_CANDIDATES_CONFIG);
    _proposalExpirationMs = config.getLong(KafkaCruiseControlConfig.PROPOSAL_EXPIRATION_MS_CONFIG);
        _proposalPrecomputingExecutor =
        Executors.newScheduledThreadPool(_numPrecomputingThreads,
                                         new KafkaCruiseControlThreadFactory("ProposalPrecomputingExecutor", false, LOG));
    _loadMonitor = loadMonitor;
    _time = time;
    _cacheLock = new ReentrantLock();
    _bestProposal = null;
    _totalProposalCandidateComputed = new AtomicInteger(0);
    _proposalComputationTimer = dropwizardMetricRegistry.timer(MetricRegistry.name("GoalOptimizer", "proposal-computation-timer"));
  }

  @Override
  public void run() {
    // We need to get this thread so it can be interrupted if the cached proposal has been invalidated.
    _proposalPrecomputingSchedulerThread = Thread.currentThread();
    LOG.info("Starting proposal candidate computation.");
    while (!_shutdown) {
      LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
      long sleepTime = _proposalExpirationMs;
      if (loadMonitorTaskRunnerState == LOADING || loadMonitorTaskRunnerState == BOOTSTRAPPING) {
        LOG.info("Skipping best proposal precomputing because load monitor is in " + loadMonitorTaskRunnerState + " state.");
        // Check in 30 seconds to see if the load monitor state has changed.
        sleepTime = 30000L;
      } else if (!_loadMonitor.meetCompletenessRequirements(_requirementsWithAvailableValidWindows)) {
        LOG.info("Skipping best proposal precomputing because load monitor does not have enough snapshots.");
        // Check in 30 seconds to see if the load monitor state has changed.
        sleepTime = 30000L;
      } else if (!validCachedProposal()) {
        LOG.debug("Invalidated cache. Cached model generation: {}, current model generation: {}",
                  _bestProposal == null ? null : _bestProposal.modelGeneration(),
                  _loadMonitor.clusterModelGeneration());
        clearBestProposal();

        long start = System.nanoTime();
        computeBestProposal();
        _proposalComputationTimer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      } else {
        LOG.debug("Skipping best proposal precomputing because the cached best result is still valid. "
                      + "Cached generation: {}", _bestProposal.modelGeneration());
      }
      long deadline = _time.milliseconds() + sleepTime;
      while (!_shutdown && _time.milliseconds() < deadline && (_bestProposal == null || validCachedProposal())) {
        try {
          Thread.sleep(deadline - _time.milliseconds());
        } catch (InterruptedException e) {
          // let it go.
        }
      }
    }
  }

  private void computeBestProposal() {
    long start = _time.milliseconds();
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < _numPrecomputingThreads; i++) {
      futures.add(_proposalPrecomputingExecutor.submit(new ProposalCandidateComputer(_goalByPriorityForPrecomputing.get(i))));
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
    LOG.info("Finished precomputation {} proposal candidates in {} ms", _totalProposalCandidateComputed.get() - 1,
             _time.milliseconds() - start);
  }

  private boolean validCachedProposal() {
    return _bestProposal != null
        && _bestProposal.modelGeneration().equals(_loadMonitor.clusterModelGeneration());
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
  public AnalyzerState state() {
    Map<Goal, Boolean> goalRediness = new LinkedHashMap<>(_goalsByPriority.size());
    for (Goal goal : _goalsByPriority.values()) {
      goalRediness.put(goal, _loadMonitor.meetCompletenessRequirements(goal.clusterModelCompletenessRequirements()));
    }
    return new AnalyzerState(_bestProposal != null, goalRediness);
  }

  /**
   * Get the cached proposals. If the cached proposal is not valid, block waiting on the cache update.
   * We do this to avoid duplicate optimization cluster model construction and proposal computation.
   */
  public OptimizerResult optimizations() {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = _loadMonitor.taskRunnerState();
    if (loadMonitorTaskRunnerState == LOADING || loadMonitorTaskRunnerState == BOOTSTRAPPING) {
      throw new IllegalStateException("Cannot get proposal because load monitor is in " + loadMonitorTaskRunnerState + " state.");
    } else if (!_loadMonitor.meetCompletenessRequirements(_requirementsWithAvailableValidWindows)) {
      throw new IllegalStateException("Cannot get proposal because model completeness is not met.");
    }
    if (!validCachedProposal()) {
      LOG.debug("Cached best proposal is not usable, Cached generation: {}, current generation: {}. Wait for cache update.",
                _bestProposal == null ? null : _bestProposal.modelGeneration(),
                _loadMonitor.clusterModelGeneration());
      synchronized (_cacheLock) {
        while (!validCachedProposal()) {
          try {
            // Wake up the proposal precomputing scheduler and wait for the cache update.
            _proposalPrecomputingSchedulerThread.interrupt();
            _cacheLock.wait();
          } catch (InterruptedException e) {
            LOG.debug("Interrupted while waiting for cache update.");
          }
        }
      }
    }
    LOG.debug("Returning optimization result from cache. Cached generation: {}",
              _bestProposal.modelGeneration());
    return _bestProposal;
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
   * @return Results of optimization containing the proposals and stats.
   * @throws KafkaCruiseControlException
   */
  public OptimizerResult optimizations(ClusterModel clusterModel) throws KafkaCruiseControlException {
    return optimizations(clusterModel, _goalsByPriority);
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
   * @return Results of optimization containing the proposals and stats.
   * @throws KafkaCruiseControlException
   */
  public OptimizerResult optimizations(ClusterModel clusterModel, Map<Integer, Goal> goalsByPriority)
      throws KafkaCruiseControlException {
    if (clusterModel == null) {
      throw new IllegalArgumentException("The cluster model cannot be null");
    }

    // Sanity check for optimizing goals.
    if (!clusterModel.isClusterAlive()) {
      throw new AnalysisInputException("All brokers are dead in the cluster.");
    }

    LOG.trace("Cluster before optimization is {}", clusterModel);
    ClusterModel.BrokerStats brokerStatsBeforeOptimization = clusterModel.brokerStats();
    Map<TopicPartition, List<Integer>> initDistribution = clusterModel.getReplicaDistribution();
    boolean isSelfHealing = !clusterModel.selfHealingEligibleReplicas().isEmpty();

    // Set of balancing proposals that will be applied to the given cluster state to satisfy goals (leadership
    // transfer AFTER partition transfer.)
    Set<Goal> optimizedGoals = new HashSet<>();
    Set<Goal> violatedGoalsBeforeOptimization = new HashSet<>();
    Set<Goal> violatedGoalsAfterOptimization = new HashSet<>();
    Map<Goal, ClusterModelStats> statsByGoalPriority = new LinkedHashMap<>();
    Map<TopicPartition, List<Integer>> preOptimizedDistribution = null;
    Set<String> excludedTopics = excludedTopics(clusterModel);
    LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
    for (Map.Entry<Integer, Goal> entry : goalsByPriority.entrySet()) {
      preOptimizedDistribution = preOptimizedDistribution == null ? initDistribution : clusterModel.getReplicaDistribution();
      Goal goal = entry.getValue();
      LOG.debug("Optimizing goal {}", goal.name());
      boolean succeeded = goal.optimize(clusterModel, optimizedGoals, excludedTopics);
      optimizedGoals.add(goal);
      statsByGoalPriority.put(goal, clusterModel.getClusterStats(_balancingConstraint));

      Set<BalancingProposal> goalProposals = AnalyzerUtils.getDiff(preOptimizedDistribution, clusterModel);
      if (!goalProposals.isEmpty() || !succeeded) {
        violatedGoalsBeforeOptimization.add(goal);
      }
      if (!succeeded) {
        violatedGoalsAfterOptimization.add(goal);
      }
      logProgress(isSelfHealing, goal.name(), optimizedGoals.size(), goalProposals);
    }

    clusterModel.sanityCheck();
    // Broker level stats in the final cluster state.
    if (LOG.isTraceEnabled()) {
      LOG.trace("Broker level stats after optimization: {}%n", clusterModel.brokerStats());
    }

    Set<BalancingProposal> proposals = AnalyzerUtils.getDiff(initDistribution, clusterModel);
    return new OptimizerResult(statsByGoalPriority,
                               violatedGoalsBeforeOptimization,
                               violatedGoalsAfterOptimization,
                               proposals,
                               brokerStatsBeforeOptimization,
                               clusterModel.brokerStats(),
                               clusterModel.generation(),
                               clusterModel.getClusterStats(_balancingConstraint));
  }

  private Set<String> excludedTopics(ClusterModel clusterModel) {
    return clusterModel.topics()
        .stream()
        .filter(topic -> _excludedTopics.matcher(topic).matches())
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
                           Set<BalancingProposal> proposals) {
    LOG.debug("[{}/{}] Generated {} proposals for {}{}.", numOptimizedGoals, _goalsByPriority.size(), proposals.size(),
              isSelfHeal ? "self-healing " : "", goalName);
    LOG.trace("Proposals for {}{}.{}%n", isSelfHeal ? "self-healing " : "", goalName, proposals);
  }

  private OptimizerResult updateBestProposal(OptimizerResult result)
      throws ModelInputException, AnalysisInputException {
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
      _totalProposalCandidateComputed.set(0);
    }
  }

  /**
   * A class for representing the results of goal optimizer. The results include stats by goal priority and
   * optimization proposals.
   */
  public static class OptimizerResult {
    private final Map<Goal, ClusterModelStats> _statsByGoalPriority;
    private final Set<BalancingProposal> _optimizationProposals;
    private final Set<Goal> _violatedGoalsBeforeOptimization;
    private final Set<Goal> _violatedGoalsAfterOptimization;
    private final ClusterModel.BrokerStats _brokerStatsBeforeOptimization;
    private final ClusterModel.BrokerStats _brokerStatsAfterOptimization;
    private final ModelGeneration _modelGeneration;
    private final ClusterModelStats _clusterModelStats;

    OptimizerResult(Map<Goal, ClusterModelStats> statsByGoalPriority,
                    Set<Goal> violatedGoalsBeforeOptimization,
                    Set<Goal> violatedGoalsAfterOptimization,
                    Set<BalancingProposal> optimizationProposals,
                    ClusterModel.BrokerStats brokerStatsBeforeOptimization,
                    ClusterModel.BrokerStats brokerStatsAfterOptimization,
                    ModelGeneration modelGeneration,
                    ClusterModelStats clusterModelStats) {
      _statsByGoalPriority = statsByGoalPriority;
      _violatedGoalsBeforeOptimization = violatedGoalsBeforeOptimization;
      _violatedGoalsAfterOptimization = violatedGoalsAfterOptimization;
      _optimizationProposals = optimizationProposals;
      _brokerStatsBeforeOptimization = brokerStatsBeforeOptimization;
      _brokerStatsAfterOptimization = brokerStatsAfterOptimization;
      _modelGeneration = modelGeneration;
      _clusterModelStats = clusterModelStats;

    }

    public Map<Goal, ClusterModelStats> statsByGoalPriority() {
      return _statsByGoalPriority;
    }

    public Set<BalancingProposal> goalProposals() {
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

      while (_totalProposalCandidateComputed.incrementAndGet() <= _maxProposalCandidates) {
        try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration()) {
          long startMs = _time.milliseconds();
          // We compute the proposal even if there is not enough modeled partitions.
          ModelCompletenessRequirements requirements = _loadMonitor.meetCompletenessRequirements(_defaultModelCompletenessRequirements) ?
              _defaultModelCompletenessRequirements : _requirementsWithAvailableValidWindows;
          ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(), requirements);
          if (!clusterModel.topics().isEmpty()) {
            OptimizerResult result = optimizations(clusterModel, _goalByPriority);
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
}
