/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;


public class AddedOrRemovedBrokerParameters extends GoalBasedOptimizationParameters {
  private List<Integer> _brokerIds;
  private Integer _concurrentPartitionMovements;
  private Integer _concurrentLeaderMovements;
  private boolean _dryRun;
  private boolean _throttleAddedOrRemovedBrokers;
  private boolean _skipHardGoalCheck;

  public AddedOrRemovedBrokerParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _brokerIds = ParameterUtils.brokerIds(_request);
    _dryRun = ParameterUtils.getDryRun(_request);
    _throttleAddedOrRemovedBrokers = ParameterUtils.throttleAddedOrRemovedBrokers(_request, _endPoint);
    _concurrentPartitionMovements = ParameterUtils.concurrentMovements(_request, true);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
  }

  public List<Integer> brokerIds() {
    return _brokerIds;
  }

  public Integer concurrentPartitionMovements() {
    return _concurrentPartitionMovements;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public boolean throttleAddedOrRemovedBrokers() {
    return _throttleAddedOrRemovedBrokers;
  }

  public boolean skipHardGoalCheck() {
    return _skipHardGoalCheck;
  }
}
