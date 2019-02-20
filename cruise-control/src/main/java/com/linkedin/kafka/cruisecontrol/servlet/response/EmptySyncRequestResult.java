/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;

public class EmptySyncRequestResult extends AbstractCruiseControlResponse {
  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
  }
}