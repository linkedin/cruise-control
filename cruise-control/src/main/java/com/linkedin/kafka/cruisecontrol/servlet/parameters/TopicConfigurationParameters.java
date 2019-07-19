/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#TOPIC_CONFIGURATION}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/topic_configuration?json=[true/false]&amp;verbose=[true/false]&amp;topic=[topic]
 *    &amp;replication_factor=[target_replication_factor]&amp;skip_rack_awareness_check=[true/false]
 *    &amp;dryRun=[true/false]&amp;goals=[goal1,goal2...]&amp;skip_hard_goal_check=[true/false]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;review_id=[id]&amp;replication_throttle=[bytes_per_second]
 * </pre>
 */
public class TopicConfigurationParameters extends GoalBasedOptimizationParameters {
  private Pattern _topic;
  private short _replicationFactor;
  private boolean _skipRackAwarenessCheck;
  private Integer _reviewId;
  private boolean _dryRun;
  private Integer _concurrentInterBrokerPartitionMovements;
  private Integer _concurrentLeaderMovements;
  private boolean _skipHardGoalCheck;
  private ReplicaMovementStrategy _replicaMovementStrategy;
  private Long _replicationThrottle;

  public TopicConfigurationParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _topic = ParameterUtils.topic(_request);
    if (_topic == null) {
      throw new IllegalArgumentException("Topic to update configuration is not specified.");
    }
    _replicationFactor = ParameterUtils.replicationFactor(_request);
    if (_replicationFactor < 1) {
      throw new IllegalArgumentException("Target replication factor cannot be set to smaller than 1.");
    }
    boolean twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_request, twoStepVerificationEnabled);
    _skipRackAwarenessCheck = ParameterUtils.skipRackAwarenessCheck(_request);
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true, false);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false, false);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_request, _config);
    _replicationThrottle = ParameterUtils.replicationThrottle(_request, _config);
  }

  public Pattern topic() {
    return _topic;
  }

  public short replicationFactor() {
    return _replicationFactor;
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

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  public Long replicationThrottle() {
    return _replicationThrottle;
  }
}