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
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
  private static final String NUM_INTER_BROKER_REPLICA_MOVEMENTS = "numReplicaMovements";
  private static final String INTER_BROKER_DATA_TO_MOVE_MB = "dataToMoveMB";
  private static final String NUM_INTRA_BROKER_REPLICA_MOVEMENTS = "numIntraBrokerReplicaMovements";
  private static final String INTRA_BROKER_DATA_TO_MOVE_MB = "intraBrokerDataToMoveMB";
  private static final String NUM_LEADER_MOVEMENTS = "numLeaderMovements";
  private static final String RECENT_WINDOWS = "recentWindows";
  private static final String MONITORED_PARTITIONS_PERCENTAGE = "monitoredPartitionsPercentage";
  private static final String EXCLUDED_TOPICS = "excludedTopics";
  private static final String EXCLUDED_BROKERS_FOR_LEADERSHIP = "excludedBrokersForLeadership";
  private static final String EXCLUDED_BROKERS_FOR_REPLICA_MOVE = "excludedBrokersForReplicaMove";
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
  private final Timer _proposalComputationTimer;
  private final ModelCompletenessRequirements _defaultModelCompletenessRequirements;
  private final ModelCompletenessRequirements _requirementsWithAvailableValidWindows;
  private final Executor _executor;
  private volatile boolean _hasOngoingExplicitPrecomputation;

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
            // Proposal precomputation runs with the default topics to exclude.
            computeCachedProposal();
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

  private void computeCachedProposal() {
    long start = _time.milliseconds();
    Future future = _proposalPrecomputingExecutor.submit(new ProposalCandidateComputer());

    try {
      boolean done = false;
      while (!_shutdown && !done) {
        try {
          future.get();
          done = true;
        } catch (InterruptedException ie) {
          LOG.debug("Goal optimizer received exception when precomputing the proposal candidates {}.", ie);
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
    for (Goal goal : _goalsByPriority) {
      goalReadiness.put(goal, _loadMonitor.meetCompletenessRequirements(clusterAndGeneration,
                                                                        goal.clusterModelCompletenessRequirements()));
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
            if (_numPrecomputingThreads > 0) {
              // Wake up the proposal precomputing scheduler and wait for the cache update.
              _proposalPrecomputingSchedulerThread.interrupt();
            } else if (!_hasOngoingExplicitPrecomputation) {
              // Submit background computation if there is no ongoing explicit precomputation and wait for the cache update.
              _hasOngoingExplicitPrecomputation = true;
              _proposalPrecomputingExecutor.submit(this::computeCachedProposal);
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
      // If the cached proposals are estimated, and the user requested for no estimation, we invalidate the cache and
      // calculate cached proposals just once. If the returned result still involves an estimation, throw an exception.
      KafkaCruiseControl.sanityCheckCapacityEstimation(allowCapacityEstimation, _cachedProposals.capacityEstimationInfoByBrokerId());
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
   * (1) using {@link #_defaultExcludedTopics}, and
   * (2) does not exclude any brokers for receiving leadership.
   * (3) does not exclude any brokers for receiving replicas.
   * (4) assumes that the optimization is not triggered by anomaly detector.
   * (5) does not specify the destination brokers for replica move explicitly.
   *
   * See {@link #optimizations(ClusterModel, List, OperationProgress, Pattern, Set, Set, boolean, Set, Map, boolean)}.
   */
  public OptimizerResult optimizations(ClusterModel clusterModel,
                                       List<Goal> goalsByPriority,
                                       OperationProgress operationProgress)
      throws KafkaCruiseControlException {
    return optimizations(clusterModel,
                         goalsByPriority,
                         operationProgress,
                         null,
                         Collections.emptySet(),
                         Collections.emptySet(),
                         false,
                         Collections.emptySet(),
                         null,
                         false);
  }

  /**
   * Depending the existence of dead/broken/decommissioned brokers in the given cluster:
   * (1) Re-balance: Generates proposals to update the state of the cluster to achieve a final balanced state.
   * (2) Self-healing: Generates proposals to move replicas away from decommissioned brokers and broken disks.
   * Returns a map from goal names to stats. Initial stats are returned under goal name "init".
   *
   * @param clusterModel The state of the cluster over which the balancing proposal will be applied. Function execution
   *                     updates the cluster state with balancing proposals. If the cluster model is specified, the
   *                     cached proposal will be ignored.
   * @param goalsByPriority the goals ordered by priority.
   * @param operationProgress to report the job progress.
   * @param requestedExcludedTopics Topics requested to be excluded from partition movement (if null,
   *                                use {@link #_defaultExcludedTopics})
   * @param excludedBrokersForLeadership Brokers excluded from receiving leadership upon proposal generation.
   * @param excludedBrokersForReplicaMove Brokers excluded from receiving replicas upon proposal generation.
   * @param isTriggeredByGoalViolation True if optimization of goals is triggered by goal violation, false otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @param initReplicaDistributionForProposalGeneration The initial replica distribution of the cluster. This is only needed
   *                                                     if the passed in clusterModel is not the original cluster model so
   *                                                     that initial replica distribution can not be deducted from that
   *                                                     cluster model, otherwise it is null. One case explicitly specifying
   *                                                     initial replica distribution needed is to increase/decrease specific
   *                                                     topic partition's replication factor, in this case some replicas
   *                                                     are tentatively deleted/added in cluster model before passing it
   *                                                     in to generate proposals.
   * @param onlyMoveImmigrantReplicas Whether restrict replica movement only to immigrant replicas or not.
   * @return Results of optimization containing the proposals and stats.
   */
  public OptimizerResult optimizations(ClusterModel clusterModel,
                                       List<Goal> goalsByPriority,
                                       OperationProgress operationProgress,
                                       Pattern requestedExcludedTopics,
                                       Set<Integer> excludedBrokersForLeadership,
                                       Set<Integer> excludedBrokersForReplicaMove,
                                       boolean isTriggeredByGoalViolation,
                                       Set<Integer> requestedDestinationBrokerIds,
                                       Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistributionForProposalGeneration,
                                       boolean onlyMoveImmigrantReplicas)
      throws KafkaCruiseControlException {
    if (clusterModel == null) {
      throw new IllegalArgumentException("The cluster model cannot be null");
    }

    // Sanity check for optimizing goals.
    if (!clusterModel.isClusterAlive()) {
      throw new IllegalArgumentException("All brokers are dead in the cluster.");
    }

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
    Map<Goal, ClusterModelStats> statsByGoalPriority = new LinkedHashMap<>(goalsByPriority.size());
    Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedReplicaDistribution = null;
    Map<TopicPartition, ReplicaPlacementInfo> preOptimizedLeaderDistribution = null;
    Set<String> excludedTopics = excludedTopics(clusterModel, requestedExcludedTopics);
    LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
    OptimizationOptions optimizationOptions = new OptimizationOptions(excludedTopics,
                                                                      excludedBrokersForLeadership,
                                                                      excludedBrokersForReplicaMove,
                                                                      isTriggeredByGoalViolation,
                                                                      requestedDestinationBrokerIds,
                                                                      onlyMoveImmigrantReplicas);
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

    clusterModel.sanityCheck();
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
                               optimizationOptions);
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
   * A class for representing the results of goal optimizer. The results include stats by goal priority and
   * optimization proposals.
   */
  public static class OptimizerResult {
    private final Map<String, Goal.ClusterModelStatsComparator> _clusterModelStatsComparatorByGoalName;
    private final Map<String, ClusterModelStats> _statsByGoalName;
    private final Set<ExecutionProposal> _proposals;
    private final Set<String> _violatedGoalNamesBeforeOptimization;
    private final Set<String> _violatedGoalNamesAfterOptimization;
    private final BrokerStats _brokerStatsBeforeOptimization;
    private final BrokerStats _brokerStatsAfterOptimization;
    private final ModelGeneration _modelGeneration;
    private final ClusterModelStats _clusterModelStats;
    private final Map<Integer, String> _capacityEstimationInfoByBrokerId;
    private final OptimizationOptions _optimizationOptions;

    OptimizerResult(Map<Goal, ClusterModelStats> statsByGoalPriority,
                    Set<String> violatedGoalNamesBeforeOptimization,
                    Set<String> violatedGoalNamesAfterOptimization,
                    Set<ExecutionProposal> proposals,
                    BrokerStats brokerStatsBeforeOptimization,
                    BrokerStats brokerStatsAfterOptimization,
                    ModelGeneration modelGeneration,
                    ClusterModelStats clusterModelStats,
                    Map<Integer, String> capacityEstimationInfoByBrokerId,
                    OptimizationOptions optimizationOptions) {
      _clusterModelStatsComparatorByGoalName = new LinkedHashMap<>(statsByGoalPriority.size());
      _statsByGoalName = new LinkedHashMap<>(statsByGoalPriority.size());
      for (Map.Entry<Goal, ClusterModelStats> entry : statsByGoalPriority.entrySet()) {
        String goalName = entry.getKey().name();
        Goal.ClusterModelStatsComparator comparator = entry.getKey().clusterModelStatsComparator();
        _clusterModelStatsComparatorByGoalName.put(goalName, comparator);
        _statsByGoalName.put(goalName, entry.getValue());
      }

      _violatedGoalNamesBeforeOptimization = violatedGoalNamesBeforeOptimization;
      _violatedGoalNamesAfterOptimization = violatedGoalNamesAfterOptimization;
      _proposals = proposals;
      _brokerStatsBeforeOptimization = brokerStatsBeforeOptimization;
      _brokerStatsAfterOptimization = brokerStatsAfterOptimization;
      _modelGeneration = modelGeneration;
      _clusterModelStats = clusterModelStats;
      _capacityEstimationInfoByBrokerId = capacityEstimationInfoByBrokerId;
      _optimizationOptions = optimizationOptions;
    }

    public Map<String, Goal.ClusterModelStatsComparator> clusterModelStatsComparatorByGoalName() {
      return _clusterModelStatsComparatorByGoalName;
    }

    public Map<String, ClusterModelStats> statsByGoalName() {
      return _statsByGoalName;
    }

    public Set<ExecutionProposal> goalProposals() {
      return _proposals;
    }

    public Set<String> violatedGoalsBeforeOptimization() {
      return _violatedGoalNamesBeforeOptimization;
    }

    public Set<String> violatedGoalsAfterOptimization() {
      return _violatedGoalNamesAfterOptimization;
    }

    public ModelGeneration modelGeneration() {
      return _modelGeneration;
    }

    public ClusterModelStats clusterModelStats() {
      return _clusterModelStats;
    }

    public BrokerStats brokerStatsBeforeOptimization() {
      return _brokerStatsBeforeOptimization;
    }

    public BrokerStats brokerStatsAfterOptimization() {
      return _brokerStatsAfterOptimization;
    }

    public boolean isCapacityEstimated() {
      return !_capacityEstimationInfoByBrokerId.isEmpty();
    }

    public Map<Integer, String> capacityEstimationInfoByBrokerId() {
      return Collections.unmodifiableMap(_capacityEstimationInfoByBrokerId);
    }

    public Set<String> excludedTopics() {
      return _optimizationOptions.excludedTopics();
    }

    public Set<Integer> excludedBrokersForLeadership() {
      return _optimizationOptions.excludedBrokersForLeadership();
    }

    public Set<Integer> excludedBrokersForReplicaMove() {
      return _optimizationOptions.excludedBrokersForReplicaMove();
    }

    /**
     * The topics of partitions which are going to be modified by proposals.
     */
    public Set<String> topicsWithReplicationFactorChange() {
      Set<String> topics = new HashSet<>(_proposals.size());
      _proposals.stream().filter(p -> p.newReplicas().size() != p.oldReplicas().size()).forEach(p -> topics.add(p.topic()));
      return topics;
    }

    private List<Number> getMovementStats() {
      int numInterBrokerReplicaMovements = 0;
      int numIntraBrokerReplicaMovements = 0;
      int numLeaderMovements = 0;
      long interBrokerDataToMove = 0L;
      long intraBrokerDataToMove = 0L;
      for (ExecutionProposal p : _proposals) {
        if (!p.replicasToAdd().isEmpty() || !p.replicasToRemove().isEmpty()) {
          numInterBrokerReplicaMovements++;
          interBrokerDataToMove += p.interBrokerDataToMoveInMB();
        } else if (!p.replicasToMoveBetweenDisksByBroker().isEmpty()) {
          numIntraBrokerReplicaMovements += p.replicasToMoveBetweenDisksByBroker().size();
          intraBrokerDataToMove += p.intraBrokerDataToMoveInMB() * p.replicasToMoveBetweenDisksByBroker().size();
        } else {
          numLeaderMovements++;
        }
      }
      return Arrays.asList(numInterBrokerReplicaMovements, interBrokerDataToMove,
                           numIntraBrokerReplicaMovements, intraBrokerDataToMove,
                           numLeaderMovements);
    }

    public String getProposalSummary() {
      List<Number> moveStats = getMovementStats();
      return String.format("%n%nOptimization has %d inter-broker replica(%d MB) moves, %d intra-broker replica(%d MB) moves"
                           + " and %d leadership moves with a cluster model of %d recent windows and %.3f%% of the partitions"
                           + " covered.%nExcluded Topics: %s.%nExcluded Brokers For Leadership: %s.%nExcluded Brokers For "
                           + "Replica Move: %s..%nCounts: %s.",
                           moveStats.get(0).intValue(), moveStats.get(1).longValue(), moveStats.get(2).intValue(),
                           moveStats.get(3).longValue(), moveStats.get(4).intValue(), _clusterModelStats.numSnapshotWindows(),
                           _clusterModelStats.monitoredPartitionsPercentage() * 100, excludedTopics(),
                           excludedBrokersForLeadership(), excludedBrokersForReplicaMove(), _clusterModelStats.toStringCounts());
    }

    public Map<String, Object> getProposalSummaryForJson() {
      List<Number> moveStats = getMovementStats();
      Map<String, Object> ret = new HashMap<>();
      ret.put(NUM_INTER_BROKER_REPLICA_MOVEMENTS, moveStats.get(0).intValue());
      ret.put(INTER_BROKER_DATA_TO_MOVE_MB, moveStats.get(1).longValue());
      ret.put(NUM_INTRA_BROKER_REPLICA_MOVEMENTS, moveStats.get(2).intValue());
      ret.put(INTRA_BROKER_DATA_TO_MOVE_MB, moveStats.get(3).longValue());
      ret.put(NUM_LEADER_MOVEMENTS, moveStats.get(4).intValue());
      ret.put(RECENT_WINDOWS, _clusterModelStats.numSnapshotWindows());
      ret.put(MONITORED_PARTITIONS_PERCENTAGE, _clusterModelStats.monitoredPartitionsPercentage() * 100.0);
      ret.put(EXCLUDED_TOPICS, excludedTopics());
      ret.put(EXCLUDED_BROKERS_FOR_LEADERSHIP, excludedBrokersForLeadership());
      ret.put(EXCLUDED_BROKERS_FOR_REPLICA_MOVE, excludedBrokersForReplicaMove());
      return ret;
    }
  }

  /**
   * A class that precomputes the proposal candidates and find the cached proposals.
   */
  private class ProposalCandidateComputer implements Runnable {
    ProposalCandidateComputer() {

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
