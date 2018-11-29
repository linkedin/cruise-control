/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#STATE}
 *
 * <pre>
 *    GET /kafkacruisecontrol/state?verbose=[true/false]&amp;substates=[SUBSTATES]&amp;super_verbose=[true/false]
 *    &amp;verbose=[true/false]&amp;json=[true/false]
 * </pre>
 */
public class CruiseControlStateParameters extends AbstractParameters {
  private Set<CruiseControlState.SubState> _substates;
  private boolean _isVerbose;
  private boolean _isSuperVerbose;

  public CruiseControlStateParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _substates = ParameterUtils.substates(_request);
    _isVerbose = ParameterUtils.isVerbose(_request);
    _isSuperVerbose = ParameterUtils.isSuperVerbose(_request);
  }

  public Set<CruiseControlState.SubState> substates() {
    return _substates;
  }

  public void setSubstates(Set<CruiseControlState.SubState> substates) {
    _substates = substates;
  }

  public boolean isVerbose() {
    return _isVerbose;
  }

  public boolean isSuperVerbose() {
    return _isSuperVerbose;
  }
}
