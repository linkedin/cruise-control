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
  private boolean _skipUnderReplicatedPartition;
  private boolean _skipReorderReplica;

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
    _skipUnderReplicatedPartition = ParameterUtils.skipUnderReplicatedPartition(_request);
    _skipReorderReplica = ParameterUtils.skipReorderReplica(_request);
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

  public boolean skipUnderReplicatedPartition() {
    return _skipUnderReplicatedPartition;
  }

  public boolean skipReorderReplica() {
    return _skipReorderReplica;
  }
}
