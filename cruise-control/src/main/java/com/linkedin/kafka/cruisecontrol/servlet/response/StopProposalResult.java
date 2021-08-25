/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.getBaseJsonString;

@JsonResponseClass
@JsonResponseExternalFields(ResponseUtils.class)
public class StopProposalResult extends AbstractCruiseControlResponse {
  public static final String STOP_EXTERNAL_AGENT_SUFFIX = " The stop_external_agent parameter would only be honored with Kafka 2.4 or above."
                                                          + " Please consider using force_stop instead.";

  public StopProposalResult(KafkaCruiseControlConfig config) {
    super(config);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    String message = "Proposal execution stopped.";
    if (!((StopProposalParameters) parameters).stopExternalAgent()) {
      message += STOP_EXTERNAL_AGENT_SUFFIX;
    }
    _cachedResponse = parameters.json() ? getBaseJsonString(message) : message;
  }
}
