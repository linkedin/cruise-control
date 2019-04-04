/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#STOP_PROPOSAL_EXECUTION}.
 *
 * <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *
 * <pre>
 * Stop the proposal execution.
 *    POST /kafkacruisecontrol/stop_proposal_execution?json=[true/false]&amp;review_id=[id]
 * </pre>
 */
public class StopProposalParameters extends AbstractParameters {
  private Integer _reviewId;
  private boolean _twoStepVerificationEnabled;

  public StopProposalParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
    _twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _reviewId = ParameterUtils.reviewId(_request, _twoStepVerificationEnabled);
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }
}
