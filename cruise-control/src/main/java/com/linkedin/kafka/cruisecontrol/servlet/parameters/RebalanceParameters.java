/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;


/**
 * Parameters for {@link CruiseControlEndPoint#REBALANCE}
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 * Trigger a workload balance.
 *    POST /kafkacruisecontrol/rebalance?dryRun=[true/false]&amp;goals=[goal1,goal2...]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]
 *    &amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]&amp;verbose=[true/false]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;exclude_recently_removed_brokers=[true/false]
 *    &amp;replica_movement_strategies=[strategy1,strategy2...]&amp;ignore_proposal_cache=[true/false]
 *    &amp;destination_broker_ids=[id1,id2...]&amp;kafka_assigner=[true/false]&amp;review_id=[id]
 * </pre>
 */
public class RebalanceParameters extends GoalBasedOptimizationParameters {
  protected boolean _dryRun;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _concurrentLeaderMovements;
  protected boolean _skipHardGoalCheck;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected boolean _ignoreProposalCache;
  protected Set<Integer> _destinationBrokerIds;
  protected Integer _reviewId;

  public RebalanceParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_request, _config);
    _ignoreProposalCache = ParameterUtils.ignoreProposalCache(_request);
    _destinationBrokerIds = ParameterUtils.destinationBrokerIds(_request);
    boolean twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_request, twoStepVerificationEnabled);
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
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

  public Set<Integer> destinationBrokerIds() {
    return _destinationBrokerIds;
  }

  public boolean ignoreProposalCache() {
    return _ignoreProposalCache;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
