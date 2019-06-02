/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#ADMIN}
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/admin?json=[true/false]&amp;disable_self_healing_for=[Set-of-{@link AnomalyType}]
 *    &amp;enable_self_healing_for=[Set-of-{@link AnomalyType}]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;review_id=[id]
 *    &amp;drop_recently_demoted_brokers=[id1,id2...]&amp;drop_recently_removed_brokers=[id1,id2...]
 * </pre>
 */
public class AdminParameters extends AbstractParameters {
  private Set<AnomalyType> _disableSelfHealingFor;
  private Set<AnomalyType> _enableSelfHealingFor;
  private Integer _concurrentInterBrokerPartitionMovements;
  private Integer _concurrentLeaderMovements;
  private Integer _reviewId;
  private final boolean _twoStepVerificationEnabled;
  private Set<Integer> _dropRecentlyRemovedBrokers;
  private Set<Integer> _dropRecentlyDemotedBrokers;

  public AdminParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
    _twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    Map<Boolean, Set<AnomalyType>> selfHealingFor = ParameterUtils.selfHealingFor(_request);
    _enableSelfHealingFor = selfHealingFor.get(true);
    _disableSelfHealingFor = selfHealingFor.get(false);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false);
    _dropRecentlyRemovedBrokers = ParameterUtils.dropRecentlyRemovedBrokers(_request);
    _dropRecentlyDemotedBrokers = ParameterUtils.dropRecentlyDemotedBrokers(_request);
    _reviewId = ParameterUtils.reviewId(_request, _twoStepVerificationEnabled);
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  public Set<AnomalyType> disableSelfHealingFor() {
    return _disableSelfHealingFor;
  }

  public Set<AnomalyType> enableSelfHealingFor() {
    return _enableSelfHealingFor;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  public Set<Integer> dropRecentlyRemovedBrokers() {
    return _dropRecentlyRemovedBrokers;
  }

  public Set<Integer> dropRecentlyDemotedBrokers() {
    return _dropRecentlyDemotedBrokers;
  }
}
