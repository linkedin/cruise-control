/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#REBALANCE}
 *
 * <pre>
 * Trigger a workload balance.
 *    POST /kafkacruisecontrol/rebalance?dryRun=[true/false]&amp;goals=[goal1,goal2...]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]
 *    &amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]&amp;verbose=[true/false]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;exclude_recently_removed_brokers=[true/false]
 *    &amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;ignore_proposal_cache=[true/false]&amp;review_id=[id]
 * </pre>
 */
public class RebalanceParameters extends GoalBasedOptimizationParameters {
  private boolean _dryRun;
  private Integer _concurrentPartitionMovements;
  private Integer _concurrentLeaderMovements;
  private boolean _skipHardGoalCheck;
  private ReplicaMovementStrategy _replicaMovementStrategy;
  private final KafkaCruiseControlConfig _config;
  private boolean _ignoreProposalCache;
  private Integer _reviewId;
  private final RebalanceParameters _reviewedParams;

  public RebalanceParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request);
    _config = config;
    _reviewedParams = null;
  }

  public RebalanceParameters(HttpServletRequest request, KafkaCruiseControlConfig config, RebalanceParameters reviewedParams) {
    super(request, reviewedParams);
    _config = config;
    _reviewedParams = reviewedParams;
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _dryRun = _reviewedParams == null ? ParameterUtils.getDryRun(_request) : _reviewedParams.dryRun();
    _concurrentPartitionMovements = _reviewedParams == null ? ParameterUtils.concurrentMovements(_request, true)
                                                            : _reviewedParams.concurrentPartitionMovements();
    _concurrentLeaderMovements = _reviewedParams == null ? ParameterUtils.concurrentMovements(_request, false)
                                                         : _reviewedParams.concurrentLeaderMovements();
    _skipHardGoalCheck = _reviewedParams == null ? ParameterUtils.skipHardGoalCheck(_request) : _reviewedParams.skipHardGoalCheck();
    _replicaMovementStrategy = _reviewedParams == null ? ParameterUtils.getReplicaMovementStrategy(_request, _config)
                                                       : _reviewedParams.replicaMovementStrategy();
    _ignoreProposalCache = _reviewedParams == null ? ParameterUtils.ignoreProposalCache(_request) : _reviewedParams.ignoreProposalCache();
    // Review id is always retrieved from the current parameters.
    boolean twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_request, twoStepVerificationEnabled);
  }

  public Integer reviewId() {
    return _reviewId;
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public Integer concurrentPartitionMovements() {
    return _concurrentPartitionMovements;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  public boolean skipHardGoalCheck() {
    return _skipHardGoalCheck;
  }

  public ReplicaMovementStrategy replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }

  public boolean ignoreProposalCache() {
    return _ignoreProposalCache;
  }
}
