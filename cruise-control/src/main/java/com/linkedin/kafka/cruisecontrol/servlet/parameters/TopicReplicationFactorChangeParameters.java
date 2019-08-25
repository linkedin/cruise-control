/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Optional parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint#TOPIC_CONFIGURATION}.
 * This class holds all the request parameters for {@link TopicConfigurationParameters.TopicConfigurationType#REPLICATION_FACTOR}.
 */
public class TopicReplicationFactorChangeParameters extends GoalBasedOptimizationParameters {

  protected Map<Short, Pattern> _topicPatternByReplicationFactor;
  protected boolean _skipRackAwarenessCheck;
  protected boolean _dryRun;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _concurrentLeaderMovements;
  protected boolean _skipHardGoalCheck;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected Long _replicationThrottle;

  private TopicReplicationFactorChangeParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _topicPatternByReplicationFactor = ParameterUtils.topicPatternByReplicationFactor(_request);
    if (_topicPatternByReplicationFactor.keySet().stream().anyMatch(rf -> rf < 1)) {
      throw new UserRequestException("Target replication factor cannot be set to smaller than 1.");
    }
    _skipRackAwarenessCheck = ParameterUtils.skipRackAwarenessCheck(_request);
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true, false);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false, false);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_request, _config);
    _replicationThrottle = ParameterUtils.replicationThrottle(_request, _config);
  }

  static TopicReplicationFactorChangeParameters maybeBuildTopicReplicationFactorChangeParameters(Map<String, ?> configs)
      throws UnsupportedEncodingException {
    TopicReplicationFactorChangeParameters topicReplicationFactorChangeParameters = new TopicReplicationFactorChangeParameters();
    topicReplicationFactorChangeParameters.configure(configs);
    topicReplicationFactorChangeParameters.initParameters();
    // If non-optional parameter is not specified in request, returns an empty instance.
    if (topicReplicationFactorChangeParameters.topicPatternByReplicationFactor().isEmpty()) {
      return null;
    }
  return topicReplicationFactorChangeParameters;
  }

  public Map<Short, Pattern> topicPatternByReplicationFactor() {
    return _topicPatternByReplicationFactor;
  }

  public boolean skipRackAwarenessCheck() {
    return _skipRackAwarenessCheck;
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

  public Long replicationThrottle() {
    return _replicationThrottle;
  }
}
