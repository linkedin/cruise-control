/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;


/**
 * Parameters for {@link CruiseControlEndPoint#STATE}
 *
 * <pre>
 *    GET /kafkacruisecontrol/state?verbose=[true/false]&amp;substates=[SUBSTATES]&amp;super_verbose=[true/false]
 *    &amp;json=[true/false]
 * </pre>
 */
public class CruiseControlStateParameters extends AbstractParameters {
  protected Set<CruiseControlState.SubState> _substates;
  protected boolean _isVerbose;
  protected boolean _isSuperVerbose;

  public CruiseControlStateParameters() {
    super();
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

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
