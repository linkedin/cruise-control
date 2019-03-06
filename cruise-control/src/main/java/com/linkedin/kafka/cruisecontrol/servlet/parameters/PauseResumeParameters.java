/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#PAUSE_SAMPLING} and
 * {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#RESUME_SAMPLING}.
 *
 * <pre>
 * 1. Pause metrics sampling. (RUNNING -&gt; PAUSED).
 *    POST /kafkacruisecontrol/pause_sampling?json=[true/false]&amp;reason=[reason-for-pause]&amp;review_id=[id]
 *
 * 2. Resume metrics sampling. (PAUSED -&gt; RUNNING).
 *    POST /kafkacruisecontrol/resume_sampling?json=[true/false]&amp;reason=[reason-for-resume]&amp;review_id=[id]
 * </pre>
 */
public class PauseResumeParameters extends AbstractParameters {
  private String _reason;
  private Integer _reviewId;
  private boolean _twoStepVerificationEnabled;
  private final PauseResumeParameters _reviewedParams;

  public PauseResumeParameters(HttpServletRequest request, boolean twoStepVerificationEnabled) {
    super(request);
    _twoStepVerificationEnabled = twoStepVerificationEnabled;
    _reviewedParams = null;
  }

  public PauseResumeParameters(HttpServletRequest request, boolean twoStepVerificationEnabled, PauseResumeParameters reviewedParams) {
    super(request, reviewedParams);
    _twoStepVerificationEnabled = twoStepVerificationEnabled;
    _reviewedParams = reviewedParams;
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _reason = _reviewedParams == null ? ParameterUtils.reason(_request) : _reviewedParams.reason();
    // Review id is always retrieved from the current parameters.
    _reviewId = ParameterUtils.reviewId(_request, _twoStepVerificationEnabled);
  }

  public String reason() {
    return _reason;
  }

  public Integer reviewId() {
    return _reviewId;
  }
}
