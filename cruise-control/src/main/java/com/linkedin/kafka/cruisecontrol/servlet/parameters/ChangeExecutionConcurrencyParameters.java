/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_LEADER_MOVEMENTS_PARAM;


/**
 * Optional Parameters for {@link CruiseControlEndPoint#ADMIN}.
 * This class holds all the request parameters for {@link AdminParameters.AdminType#CHANGE_CONCURRENCY}.
 */
public class ChangeExecutionConcurrencyParameters  extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM);
    validParameterNames.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    validParameterNames.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected Long _executionProgressCheckIntervalMs;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _concurrentLeaderMovements;

  protected ChangeExecutionConcurrencyParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _executionProgressCheckIntervalMs = ParameterUtils.executionProgressCheckIntervalMs(_request);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false);
  }

  /**
   * Create a {@link ChangeExecutionConcurrencyParameters} object from the request.
   *
   * @param configs Information collected from request and Cruise Control configs.
   * @return a ChangeExecutionConcurrencyParameters object; or null if any required parameters is not specified in the request.
   */
  public static ChangeExecutionConcurrencyParameters maybeBuildChangeExecutionConcurrencyParameters(Map<String, ?> configs)
      throws UnsupportedEncodingException {
    ChangeExecutionConcurrencyParameters changeExecutionConcurrencyParameters = new ChangeExecutionConcurrencyParameters();
    changeExecutionConcurrencyParameters.configure(configs);
    changeExecutionConcurrencyParameters.initParameters();
    // At least new concurrency for one type of task should be explicitly specified in the request; otherwise, return null.
    if (changeExecutionConcurrencyParameters.executionProgressCheckIntervalMs() == null
        && changeExecutionConcurrencyParameters.concurrentInterBrokerPartitionMovements() == null
        && changeExecutionConcurrencyParameters.concurrentLeaderMovements() == null) {
      return null;
    }
    return changeExecutionConcurrencyParameters;
  }

  public Long executionProgressCheckIntervalMs() {
    return _executionProgressCheckIntervalMs;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
