/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;

/**
 * Optional Parameters for {@link CruiseControlEndPoint#ADMIN}.
 * This class holds all the request parameters for {@link AdminParameters.AdminType#UPDATE_SELF_HEALING}.
 */
public class UpdateSelfHealingParameters extends AbstractParameters {
  protected Set<AnomalyType> _disableSelfHealingFor;
  protected Set<AnomalyType> _enableSelfHealingFor;

  protected UpdateSelfHealingParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    Map<Boolean, Set<AnomalyType>> selfHealingFor = ParameterUtils.selfHealingFor(_request);
    _enableSelfHealingFor = selfHealingFor.get(true);
    _disableSelfHealingFor = selfHealingFor.get(false);
  }

  /**
   * Try to create a UpdateSelfHealingParameters object from the request.
   *
   * @param configs Information collected from request and Cruise Control configs.
   * @return a UpdateSelfHealingParameters object; or null if any required parameter is not specified in the request.
   */
  static UpdateSelfHealingParameters maybeBuildUpdateSelfHealingParameters(Map<String, ?> configs)
      throws UnsupportedEncodingException {
    UpdateSelfHealingParameters selfHealingUpdateParameters = new UpdateSelfHealingParameters();
    selfHealingUpdateParameters.configure(configs);
    selfHealingUpdateParameters.initParameters();
    // At least self-healing for one anomaly type should requested to enable/disable; otherwise, return null.
    if (selfHealingUpdateParameters.enableSelfHealingFor().isEmpty()
        && selfHealingUpdateParameters.disableSelfHealingFor().isEmpty()) {
      return null;
    }
    return selfHealingUpdateParameters;
  }

  public Set<AnomalyType> disableSelfHealingFor() {
    return _disableSelfHealingFor;
  }

  public Set<AnomalyType> enableSelfHealingFor() {
    return _enableSelfHealingFor;
  }
}
