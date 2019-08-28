/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SUBSTATES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.VERBOSE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SUPER_VERBOSE_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#STATE}
 *
 * <pre>
 *    GET /kafkacruisecontrol/state?verbose=[true/false]&amp;substates=[SUBSTATES]&amp;super_verbose=[true/false]
 *    &amp;json=[true/false]
 * </pre>
 */
public class CruiseControlStateParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(SUBSTATES_PARAM);
    validParameterNames.add(VERBOSE_PARAM);
    validParameterNames.add(SUPER_VERBOSE_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
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

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
