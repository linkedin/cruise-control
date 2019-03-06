/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


public abstract class KafkaOptimizationParameters extends AbstractParameters {
  protected boolean _allowCapacityEstimation;
  protected boolean _isVerbose;
  protected boolean _excludeRecentlyDemotedBrokers;
  private final KafkaOptimizationParameters _reviewedParams;

  KafkaOptimizationParameters(HttpServletRequest request) {
    super(request);
    _reviewedParams = null;
  }

  KafkaOptimizationParameters(HttpServletRequest request, KafkaOptimizationParameters reviewedParams) {
    super(request, reviewedParams);
    _reviewedParams = reviewedParams;
  }
  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _allowCapacityEstimation = _reviewedParams == null ? ParameterUtils.allowCapacityEstimation(_request)
                                                       : _reviewedParams.allowCapacityEstimation();
    _isVerbose = _reviewedParams == null ? ParameterUtils.isVerbose(_request)
                                         : _reviewedParams.isVerbose();
    _excludeRecentlyDemotedBrokers = _reviewedParams == null ? ParameterUtils.excludeRecentlyDemotedBrokers(_request)
                                                             : _reviewedParams.excludeRecentlyDemotedBrokers();
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
}