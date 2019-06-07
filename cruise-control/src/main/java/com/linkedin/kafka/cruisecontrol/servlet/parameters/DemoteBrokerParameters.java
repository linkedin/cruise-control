/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#DEMOTE_BROKER}
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *   <li>Note that "brokerid_and_logdirs" takes comma as delimiter between two broker id and logdir pairs -- i.e. we assume
 *   a valid logdir name contains no comma.</li>
 * </ul>
 *
 * <pre>
 * Demote a broker
 *    POST /kafkacruisecontrol/demote_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]
 *    &amp;skip_urp_demotion=[true/false]&amp;exclude_follower_demotion=[true/false]&amp;verbose=[true/false]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;brokerid_and_logdirs=[broker_id1-logdir1,broker_id2-logdir2]&amp;review_id=[id]
 *    &amp;replication_throttle=[bytes_per_second]
 * </pre>
 */
public class DemoteBrokerParameters extends KafkaOptimizationParameters {
  private boolean _dryRun;
  private Set<Integer> _brokerIds;
  private Integer _concurrentLeaderMovements;
  private boolean _skipUrpDemotion;
  private boolean _excludeFollowerDemotion;
  private ReplicaMovementStrategy _replicaMovementStrategy;
  private Long _replicationThrottle;
  private Integer _reviewId;
  private Map<Integer, Set<String>> _logdirByBrokerId;

  public DemoteBrokerParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _brokerIds = ParameterUtils.brokerIds(_request);
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false, false);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_request);
    _skipUrpDemotion = ParameterUtils.skipUrpDemotion(_request);
    _excludeFollowerDemotion = ParameterUtils.excludeFollowerDemotion(_request);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_request, _config);
    _replicationThrottle = ParameterUtils.replicationThrottle(_request, _config);
    boolean twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_request, twoStepVerificationEnabled);
    _logdirByBrokerId = ParameterUtils.brokerIdAndLogdirs(_request);
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

  public Map<Integer, Set<String>> brokerIdAndLogdirs() {
    return _logdirByBrokerId;
  }

  public ReplicaMovementStrategy replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }

  public Long replicationThrottle() {
    return _replicationThrottle;
  }
}
