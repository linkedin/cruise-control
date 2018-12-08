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
 *    POST /kafkacruisecontrol/pause_sampling?json=[true/false]&amp;reason=[reason-for-pause]
 *
 * 2. Resume metrics sampling. (PAUSED -&gt; RUNNING).
 *    POST /kafkacruisecontrol/resume_sampling?json=[true/false]&amp;reason=[reason-for-resume]
 * </pre>
 */
public class PauseResumeParameters extends AbstractParameters {
  private String _reason;

  public PauseResumeParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _reason = ParameterUtils.reason(_request);
  }

  public String reason() {
    return _reason;
  }
}
