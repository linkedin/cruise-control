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
 * Parameters for {@link CruiseControlEndPoint#DEMOTE_BROKER}
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 * Demote a broker
 *    POST /kafkacruisecontrol/demote_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]
 *    &amp;skip_urp_demotion=[true/false]&amp;exclude_follower_demotion=[true/false]&amp;verbose=[true/false]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;review_id=[id]
 * </pre>
 */
public class DemoteBrokerParameters extends KafkaOptimizationParameters {
  protected boolean _dryRun;
  protected Set<Integer> _brokerIds;
  protected Integer _concurrentLeaderMovements;
  protected boolean _skipUrpDemotion;
  protected boolean _excludeFollowerDemotion;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected Integer _reviewId;

  public DemoteBrokerParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _brokerIds = ParameterUtils.brokerIds(_request, false);
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_request);
    _skipUrpDemotion = ParameterUtils.skipUrpDemotion(_request);
    _excludeFollowerDemotion = ParameterUtils.excludeFollowerDemotion(_request);
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

  public boolean dryRun() {
    return _dryRun;
  }

  public Set<Integer> brokerIds() {
    return _brokerIds;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  public boolean skipUrpDemotion() {
    return _skipUrpDemotion;
  }

  public boolean excludeFollowerDemotion() {
    return _excludeFollowerDemotion;
  }

  public ReplicaMovementStrategy replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
