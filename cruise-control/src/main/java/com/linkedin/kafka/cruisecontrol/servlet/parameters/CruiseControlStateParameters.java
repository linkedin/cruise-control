/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaCruiseControlState;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


public class CruiseControlStateParameters extends AbstractCruiseControlParameters {
  private Set<KafkaCruiseControlState.SubState> _substates;
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

  public Set<KafkaCruiseControlState.SubState> substates() {
    return _substates;
  }

  public void setSubstates(Set<KafkaCruiseControlState.SubState> substates) {
    _substates = substates;
  }

  public boolean isVerbose() {
    return _isVerbose;
  }

  public boolean isSuperVerbose() {
    return _isSuperVerbose;
  }
}
