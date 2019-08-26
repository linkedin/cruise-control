/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;


/**
 * Optional Parameters for {@link CruiseControlEndPoint#ADMIN}.
 * This class holds all the request parameters for {@link AdminParameters.AdminType#CHANGE_CONCURRENCY}.
 */
public class ChangeExecutionConcurrencyParameters  extends AbstractParameters {
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _concurrentIntraBrokerPartitionMovements;
  protected Integer _concurrentLeaderMovements;

  protected ChangeExecutionConcurrencyParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, true, false);
    _concurrentIntraBrokerPartitionMovements = ParameterUtils.concurrentMovements(_request, false, true);
    _concurrentLeaderMovements = ParameterUtils.concurrentMovements(_request, false, false);
  }

  /**
   * Create a {@link ChangeExecutionConcurrencyParameters} object from the request.
   *
   * @param configs Information collected from request and Cruise Control configs.
   * @return a ChangeExecutionConcurrencyParameters object; or null if any required parameters is not specified in the request.
   */
  static ChangeExecutionConcurrencyParameters maybeBuildChangeExecutionConcurrencyParameters(Map<String, ?> configs)
      throws UnsupportedEncodingException {
    ChangeExecutionConcurrencyParameters changeExecutionConcurrencyParameters = new ChangeExecutionConcurrencyParameters();
    changeExecutionConcurrencyParameters.configure(configs);
    changeExecutionConcurrencyParameters.initParameters();
    // At least new concurrency for one type of task should be explicitly specified in the request; otherwise, return null.
    if (changeExecutionConcurrencyParameters.concurrentInterBrokerPartitionMovements() == null
        && changeExecutionConcurrencyParameters.concurrentIntraBrokerPartitionMovements() == null
        && changeExecutionConcurrencyParameters.concurrentLeaderMovements() == null) {
      return null;
    }
    return changeExecutionConcurrencyParameters;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer concurrentIntraBrokerPartitionMovements() {
    return _concurrentIntraBrokerPartitionMovements;
  }

  public Integer concurrentLeaderMovements() {
    return _concurrentLeaderMovements;
  }
}
