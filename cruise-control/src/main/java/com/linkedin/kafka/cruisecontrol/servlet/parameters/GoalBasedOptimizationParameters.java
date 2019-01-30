/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;


public abstract class GoalBasedOptimizationParameters extends KafkaOptimizationParameters {
  protected ParameterUtils.DataFrom _dataFrom;
  protected boolean _useReadyDefaultGoals;
  protected List<String> _goals;
  protected Pattern _excludedTopics;
  protected boolean _excludeRecentlyRemovedBrokers;

  GoalBasedOptimizationParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _dataFrom = ParameterUtils.getDataFrom(_request);
    _useReadyDefaultGoals = ParameterUtils.useReadyDefaultGoals(_request);
    _goals = ParameterUtils.getGoals(_request);
    _excludedTopics = ParameterUtils.excludedTopics(_request);
    _excludeRecentlyRemovedBrokers = ParameterUtils.excludeRecentlyRemovedBrokers(_request);
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

  public boolean excludeRecentlyRemovedBrokers() {
    return _excludeRecentlyRemovedBrokers;
  }

  public static class GoalsAndRequirements {
    private final List<String> _goals;
    private final ModelCompletenessRequirements _requirements;

    public GoalsAndRequirements(List<String> goals, ModelCompletenessRequirements requirements) {
      _goals = goals; // An empty list indicates the default goals.
      _requirements = requirements;
    }

    public List<String> goals() {
      return _goals;
    }

    public ModelCompletenessRequirements requirements() {
      return _requirements;
    }
  }
}
