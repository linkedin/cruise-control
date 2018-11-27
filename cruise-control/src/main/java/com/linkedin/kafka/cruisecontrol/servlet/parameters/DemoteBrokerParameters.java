/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#DEMOTE_BROKER}
 */
public class DemoteBrokerParameters extends KafkaOptimizationParameters {
  private boolean _dryRun;
  private List<Integer> _brokerIds;
  private Integer _concurrentLeaderMovements;
  private boolean _skipUrpDemotion;
  private boolean _excludeFollowerDemotion;

  public DemoteBrokerParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _brokerIds = ParameterUtils.brokerIds(_request);
    _dryRun = ParameterUtils.getDryRun(_request);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_request);
    _skipUrpDemotion = ParameterUtils.skipUrpDemotion(_request);
    _excludeFollowerDemotion = ParameterUtils.excludeFollowerDemotion(_request);
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public List<Integer> brokerIds() {
    return _brokerIds;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  public boolean skipUrpDemotion() {
    return _skipUrpDemotion;
  }

  public boolean excludeFollowerDemotion() {
    return _excludeFollowerDemotion;
  }
}
