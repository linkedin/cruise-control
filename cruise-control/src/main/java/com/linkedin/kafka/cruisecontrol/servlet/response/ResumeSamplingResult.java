/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.getBaseJSONString;


public class ResumeSamplingResult extends AbstractCruiseControlResponse {

  public ResumeSamplingResult(KafkaCruiseControlConfig config) {
    super(config);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    String message = "Metric sampling resumed.";
    _cachedResponse = parameters.json() ? getBaseJSONString(message) : message;
  }
}
