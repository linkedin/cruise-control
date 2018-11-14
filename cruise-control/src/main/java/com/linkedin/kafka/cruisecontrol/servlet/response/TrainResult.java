/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.getBaseJSONString;


public class TrainResult extends AbstractCruiseControlResponse {

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    String message = String.format("Load model training started. Check status through the %s endpoint", EndPoint.STATE);
    _cachedResponse = parameters.json() ? getBaseJSONString(message) : message;
  }
}
