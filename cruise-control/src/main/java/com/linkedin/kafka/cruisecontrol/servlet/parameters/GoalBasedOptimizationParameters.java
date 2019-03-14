/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;


public abstract class GoalBasedOptimizationParameters extends KafkaOptimizationParameters {
  private static final boolean INCLUDE_ALL_TOPICS = true;
  private static final Map<ParameterUtils.DataFrom, Integer> MIN_NUM_VALID_WINDOWS = new HashMap<>(2);
  private static final Map<ParameterUtils.DataFrom, Double> MIN_VALID_PARTITIONS_RATIO = new HashMap<>(2);
  static {
    MIN_NUM_VALID_WINDOWS.put(ParameterUtils.DataFrom.VALID_PARTITIONS, Integer.MAX_VALUE);
    MIN_NUM_VALID_WINDOWS.put(ParameterUtils.DataFrom.VALID_WINDOWS, 1);
  }
  static {
    MIN_VALID_PARTITIONS_RATIO.put(ParameterUtils.DataFrom.VALID_PARTITIONS, 0.0);
    MIN_VALID_PARTITIONS_RATIO.put(ParameterUtils.DataFrom.VALID_WINDOWS, 1.0);
  }
  protected ParameterUtils.DataFrom _dataFrom;
  protected boolean _useReadyDefaultGoals;
  protected Pattern _excludedTopics;
  protected boolean _excludeRecentlyRemovedBrokers;
  protected GoalsAndRequirements _goalsAndRequirements;

  GoalBasedOptimizationParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _dataFrom = ParameterUtils.getDataFrom(_request);
    _useReadyDefaultGoals = ParameterUtils.useReadyDefaultGoals(_request);
    _excludedTopics = ParameterUtils.excludedTopics(_request);
    _excludeRecentlyRemovedBrokers = ParameterUtils.excludeRecentlyRemovedBrokers(_request);
    List<String> goals = ParameterUtils.getGoals(_request);
    _goalsAndRequirements = new GoalsAndRequirements(goals, getRequirements(_dataFrom));
  }

  public ParameterUtils.DataFrom dataFrom() {
    return _dataFrom;
  }

  public boolean useReadyDefaultGoals() {
    return _useReadyDefaultGoals;
  }

  public List<String> goals() {
    return _goalsAndRequirements.goals();
  }

  public ModelCompletenessRequirements modelCompletenessRequirements() {
    return _goalsAndRequirements.requirements();
  }

  public Pattern excludedTopics() {
    return _excludedTopics;
  }

  public boolean excludeRecentlyRemovedBrokers() {
    return _excludeRecentlyRemovedBrokers;
  }

  private static ModelCompletenessRequirements getRequirements(ParameterUtils.DataFrom dataFrom) {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS.get(dataFrom),
                                             MIN_VALID_PARTITIONS_RATIO.get(dataFrom),
                                             INCLUDE_ALL_TOPICS);
  }
}
