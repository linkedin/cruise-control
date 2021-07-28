/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.getBaseJSONString;

@JsonResponseClass
@JsonResponseExternalFields(ResponseUtils.class)
public class StopProposalResult extends AbstractCruiseControlResponse {
  public static final String FORCE_STOP_SUFFIX = " For replica reassignments force-stop is deprecated and acts as a graceful stop.";

  public StopProposalResult(KafkaCruiseControlConfig config) {
    super(config);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    String message = "Proposal execution stopped.";
    if (((StopProposalParameters) parameters).forceExecutionStop()) {
      message += FORCE_STOP_SUFFIX;
    }
    _cachedResponse = parameters.json() ? getBaseJSONString(message) : message;
  }
}
