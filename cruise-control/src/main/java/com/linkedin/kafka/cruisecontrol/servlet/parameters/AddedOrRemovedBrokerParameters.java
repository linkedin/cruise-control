/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;


public class AddedOrRemovedBrokerParameters extends KafkaOptimizationParameters {
  private List<Integer> _brokerIds;
  private Integer _concurrentPartitionMovements;
  private Integer _concurrentLeaderMovements;
  private boolean _dryRun;
  private boolean _throttleAddedOrRemovedBrokers;
  private boolean _skipHardGoalCheck;
  private ParameterUtils.DataFrom _dataFrom;
  private boolean _useReadyDefaultGoals;
  private List<String> _goals;
  private Pattern _excludedTopics;

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
    _dataFrom = ParameterUtils.getDataFrom(_request);
    _useReadyDefaultGoals = ParameterUtils.useReadyDefaultGoals(_request);
    _goals = ParameterUtils.getGoals(_request);
    _excludedTopics = ParameterUtils.excludedTopics(_request);
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

  public ParameterUtils.DataFrom dataFrom() {
    return _dataFrom;
  }

  public boolean useReadyDefaultGoals() {
    return _useReadyDefaultGoals;
  }

  public List<String> goals() {
    return _goals;
  }

  public Pattern excludedTopics() {
    return _excludedTopics;
  }
}
