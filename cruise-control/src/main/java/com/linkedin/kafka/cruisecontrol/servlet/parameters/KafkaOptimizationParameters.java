/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import java.util.Map;


public abstract class KafkaOptimizationParameters extends AbstractParameters {
  protected boolean _allowCapacityEstimation;
  protected boolean _isVerbose;
  protected boolean _excludeRecentlyDemotedBrokers;

  KafkaOptimizationParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_request);
    _isVerbose = ParameterUtils.isVerbose(_request);
    _excludeRecentlyDemotedBrokers = ParameterUtils.excludeRecentlyDemotedBrokers(_request);
  }

  public boolean allowCapacityEstimation() {
    return _allowCapacityEstimation;
  }

  public boolean isVerbose() {
    return _isVerbose;
  }

  public boolean excludeRecentlyDemotedBrokers() {
    return _excludeRecentlyDemotedBrokers;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}