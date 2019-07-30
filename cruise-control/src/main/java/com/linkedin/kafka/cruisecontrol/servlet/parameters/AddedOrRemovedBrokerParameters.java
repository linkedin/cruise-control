/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;


public abstract class AddedOrRemovedBrokerParameters extends GoalBasedOptimizationParameters {
  protected Set<Integer> _brokerIds;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _concurrentLeaderMovements;
  protected boolean _dryRun;
  protected Long _replicationThrottle;
  protected boolean _skipHardGoalCheck;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected Integer _reviewId;

  public AddedOrRemovedBrokerParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _brokerIds = ParameterUtils.brokerIds(_request, false);
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true, false);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false, false);
    _replicationThrottle = ParameterUtils.replicationThrottle(_request, _config);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_request, _config);
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

  public Set<Integer> brokerIds() {
    return _brokerIds;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public Long replicationThrottle() {
    return _replicationThrottle;
  }

  public boolean skipHardGoalCheck() {
    return _skipHardGoalCheck;
  }

  public ReplicaMovementStrategy replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
