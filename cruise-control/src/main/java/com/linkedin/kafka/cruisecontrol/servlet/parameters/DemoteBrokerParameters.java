/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.io.UnsupportedEncodingException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#DEMOTE_BROKER}
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
  private boolean _dryRun;
  private List<Integer> _brokerIds;
  private Integer _concurrentLeaderMovements;
  private boolean _skipUrpDemotion;
  private boolean _excludeFollowerDemotion;
  private ReplicaMovementStrategy _replicaMovementStrategy;
  private KafkaCruiseControlConfig _config;
  private Integer _reviewId;
  private final DemoteBrokerParameters _reviewedParams;

  public DemoteBrokerParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request);
    _config = config;
    _reviewedParams = null;
  }

  public DemoteBrokerParameters(HttpServletRequest request, KafkaCruiseControlConfig config, DemoteBrokerParameters reviewedParams) {
    super(request, reviewedParams);
    _config = config;
    _reviewedParams = reviewedParams;
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _brokerIds = _reviewedParams == null ? ParameterUtils.brokerIds(_request) : _reviewedParams.brokerIds();
    _dryRun = _reviewedParams == null ? ParameterUtils.getDryRun(_request) : _reviewedParams.dryRun();
    _concurrentLeaderMovements = _reviewedParams == null ? ParameterUtils.concurrentMovements(_request, false)
                                                         : _reviewedParams.concurrentLeaderMovements();
    _allowCapacityEstimation = _reviewedParams == null ? ParameterUtils.allowCapacityEstimation(_request)
                                                       : _reviewedParams.allowCapacityEstimation();
    _skipUrpDemotion = _reviewedParams == null ? ParameterUtils.skipUrpDemotion(_request) : _reviewedParams.skipUrpDemotion();
    _excludeFollowerDemotion = _reviewedParams == null ? ParameterUtils.excludeFollowerDemotion(_request)
                                                       : _reviewedParams.excludeFollowerDemotion();
    _replicaMovementStrategy = _reviewedParams == null ? ParameterUtils.getReplicaMovementStrategy(_request, _config)
                                                       : _reviewedParams.replicaMovementStrategy();
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

  public List<Integer> brokerIds() {
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
}
