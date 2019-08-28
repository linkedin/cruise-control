/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DATA_FROM_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.USE_READY_DEFAULT_GOALS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.EXCLUDED_TOPICS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.GOALS_PARAM;


public abstract class GoalBasedOptimizationParameters extends KafkaOptimizationParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(DATA_FROM_PARAM);
    validParameterNames.add(USE_READY_DEFAULT_GOALS_PARAM);
    validParameterNames.add(EXCLUDED_TOPICS_PARAM);
    validParameterNames.add(EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM);
    validParameterNames.add(GOALS_PARAM);
    validParameterNames.addAll(KafkaOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
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

  GoalBasedOptimizationParameters() {
    super();
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

  protected static ModelCompletenessRequirements getRequirements(ParameterUtils.DataFrom dataFrom) {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS.get(dataFrom),
                                             MIN_VALID_PARTITIONS_RATIO.get(dataFrom),
                                             INCLUDE_ALL_TOPICS);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
