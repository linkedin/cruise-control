/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#ADMIN}
 *
 * <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *
 * <pre>
 *    POST /kafkacruisecontrol/admin?json=[true/false]&amp;disable_self_healing_for=[Set-of-{@link AnomalyType}]
 *    &amp;enable_self_healing_for=[Set-of-{@link AnomalyType}]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;review_id=[id]
 * </pre>
 */
public class AdminParameters extends AbstractParameters {
  private Set<AnomalyType> _disableSelfHealingFor;
  private Set<AnomalyType> _enableSelfHealingFor;
  private Integer _concurrentInterBrokerPartitionMovements;
  private Integer _concurrentLeaderMovements;
  private Integer _reviewId;
  private boolean _twoStepVerificationEnabled;

  public AdminParameters(HttpServletRequest request, boolean twoStepVerificationEnabled) {
    super(request);
    _twoStepVerificationEnabled = twoStepVerificationEnabled;
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    Map<Boolean, Set<AnomalyType>> selfHealingFor = ParameterUtils.selfHealingFor(_request);
    _enableSelfHealingFor = selfHealingFor.get(true);
    _disableSelfHealingFor = selfHealingFor.get(false);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false);
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
}
