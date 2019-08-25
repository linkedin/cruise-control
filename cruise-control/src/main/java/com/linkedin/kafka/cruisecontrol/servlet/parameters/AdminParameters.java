/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.sanityCheckOptionalParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ChangeExecutionConcurrencyParameters.maybeBuildChangeExecutionConcurrencyParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.DropRecentBrokersParameters.maybeBuildDropRecentBrokersParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.UpdateSelfHealingParameters.maybeBuildUpdateSelfHealingParameters;


/**
 * Parameters for {@link CruiseControlEndPoint#ADMIN}
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/admin?json=[true/false]&amp;disable_self_healing_for=[Set-of-{@link AnomalyType}]
 *    &amp;enable_self_healing_for=[Set-of-{@link AnomalyType}]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_intra_broker_partition_movements=[POSITIVE-INTEGER]&amp;concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;review_id=[id]&amp;drop_recently_demoted_brokers=[id1,id2...]&amp;drop_recently_removed_brokers=[id1,id2...]
 * </pre>
 */
public class AdminParameters extends AbstractParameters {
  protected Integer _reviewId;
  protected Map<String, ?> _configs;
  protected DropRecentBrokersParameters _dropBrokersParameters;
  protected UpdateSelfHealingParameters _updateSelfHealingParameters;
  protected ChangeExecutionConcurrencyParameters _changeExecutionConcurrencyParameters;

  public AdminParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    boolean twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_request, twoStepVerificationEnabled);
    _dropBrokersParameters = maybeBuildDropRecentBrokersParameters(_configs);
    _updateSelfHealingParameters = maybeBuildUpdateSelfHealingParameters(_configs);
    _changeExecutionConcurrencyParameters = maybeBuildChangeExecutionConcurrencyParameters(_configs);
    sanityCheckOptionalParameters(_dropBrokersParameters, _updateSelfHealingParameters, _changeExecutionConcurrencyParameters);
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  public DropRecentBrokersParameters dropRecentBrokersParameters() {
    return _dropBrokersParameters;
  }

  public UpdateSelfHealingParameters updateSelfHealingParameters() {
    return _updateSelfHealingParameters;
  }

  public ChangeExecutionConcurrencyParameters changeExecutionConcurrencyParameters() {
    return _changeExecutionConcurrencyParameters;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _configs = configs;
  }

  /**
   * Supported topic configuration type to be changed via {@link CruiseControlEndPoint#ADMIN} endpoint.
   */
  public enum AdminType {
    UPDATE_SELF_HEALING, CHANGE_CONCURRENCY, DROP_RECENT_BROKERS
  }
}
