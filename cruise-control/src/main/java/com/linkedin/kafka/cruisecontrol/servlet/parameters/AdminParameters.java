/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#ADMIN}
 *
 * <pre>
 *    POST /kafkacruisecontrol/admin?json=[true/false]&amp;disable_self_healing_for=[Set-of-{@link AnomalyType}]
 *    &amp;enable_self_healing_for=[Set-of-{@link AnomalyType}]
 * </pre>
 */
public class AdminParameters extends AbstractParameters {
  private Set<AnomalyType> _disableSelfHealingFor;
  private Set<AnomalyType> _enableSelfHealingFor;

  public AdminParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    Map<Boolean, Set<AnomalyType>> selfHealingFor = ParameterUtils.selfHealingFor(_request);
    _enableSelfHealingFor = selfHealingFor.get(true);
    _disableSelfHealingFor = selfHealingFor.get(false);
  }

  public Set<AnomalyType> disableSelfHealingFor() {
    return _disableSelfHealingFor;
  }

  public Set<AnomalyType> enableSelfHealingFor() {
    return _enableSelfHealingFor;
  }
}
