/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#BOOTSTRAP}
 *
 * <pre>
 * 1. RANGE MODE:
 *    GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]&amp;clearmetrics=[true/false]
 *    &amp;json=[true/false]
 * 2. SINCE MODE:
 *    GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&amp;clearmetrics=[true/false]&amp;json=[true/false]
 * 3. RECENT MODE:
 *    GET /kafkacruisecontrol/bootstrap?clearmetrics=[true/false]&amp;json=[true/false]
 * </pre>
 */
public class BootstrapParameters extends AbstractParameters {
  private Long _startMs;
  private Long _endMs;
  private boolean _clearMetrics;

  public BootstrapParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _startMs = ParameterUtils.startMs(_request);
    _endMs = ParameterUtils.endMs(_request);
    _clearMetrics = ParameterUtils.clearMetrics(_request);
    if (_startMs == null && _endMs != null) {
      throw new UserRequestException("The start time cannot be empty when end time is specified.");
    }
  }

  public Long startMs() {
    return _startMs;
  }

  public Long endMs() {
    return _endMs;
  }

  public boolean clearMetrics() {
    return _clearMetrics;
  }
}
