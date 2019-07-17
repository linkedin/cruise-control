/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;


/**
 * Parameters for {@link CruiseControlEndPoint#PAUSE_SAMPLING} and
 * {@link CruiseControlEndPoint#RESUME_SAMPLING}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
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

  public PauseResumeParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _reason = ParameterUtils.reason(_request);
    _reviewId = ParameterUtils.reviewId(_request, _twoStepVerificationEnabled);
  }

  public String reason() {
    return _reason;
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
  }
}
