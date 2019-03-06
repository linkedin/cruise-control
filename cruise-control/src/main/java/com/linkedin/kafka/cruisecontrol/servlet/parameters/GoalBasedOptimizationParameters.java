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
  private final GoalBasedOptimizationParameters _reviewedParams;

  GoalBasedOptimizationParameters(HttpServletRequest request) {
    super(request);
    _reviewedParams = null;
  }

  GoalBasedOptimizationParameters(HttpServletRequest request, GoalBasedOptimizationParameters reviewedParams) {
    super(request, reviewedParams);
    _reviewedParams = reviewedParams;
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _dataFrom = _reviewedParams == null ? ParameterUtils.getDataFrom(_request) : _reviewedParams.dataFrom();
    _useReadyDefaultGoals = _reviewedParams == null ? ParameterUtils.useReadyDefaultGoals(_request)
                                                    : _reviewedParams.useReadyDefaultGoals();
    _excludedTopics = _reviewedParams == null ? ParameterUtils.excludedTopics(_request)
                                              : _reviewedParams.excludedTopics();
    _excludeRecentlyRemovedBrokers = _reviewedParams == null ? ParameterUtils.excludeRecentlyRemovedBrokers(_request)
                                                             : _reviewedParams.excludeRecentlyRemovedBrokers();
    List<String> goals = _reviewedParams == null ? ParameterUtils.getGoals(_request) : _reviewedParams.goals();
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
