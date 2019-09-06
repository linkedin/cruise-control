/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.shouldRefreshClusterAndGeneration;
import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.*;


/**
 * The async runnable for Cruise Control state.
 */
public class GetStateRunnable extends OperationRunnable {
  protected final Set<CruiseControlState.SubState> _substates;

  public GetStateRunnable(KafkaCruiseControl kafkaCruiseControl,
                          OperationFuture future,
                          CruiseControlStateParameters parameters) {
    super(kafkaCruiseControl, future);
    _substates = parameters.substates();
  }

  /**
   * Get the state with selected substates for Kafka Cruise Control.
   */
  @Override
  public CruiseControlState getResult() {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = null;
    // In case no substate is specified, return all substates.
    Set<CruiseControlState.SubState> substates = !_substates.isEmpty() ? _substates
                                                                       : new HashSet<>(Arrays.asList(CruiseControlState.SubState.values()));

    if (shouldRefreshClusterAndGeneration(substates)) {
      clusterAndGeneration = _kafkaCruiseControl.refreshClusterAndGeneration();
    }

    return new CruiseControlState(substates.contains(EXECUTOR) ? _kafkaCruiseControl.executorState() : null,
                                  substates.contains(MONITOR) ? _kafkaCruiseControl.monitorState(_future.operationProgress(),
                                                                                                 clusterAndGeneration) : null,
                                  substates.contains(ANALYZER) ? _kafkaCruiseControl.analyzerState(clusterAndGeneration) : null,
                                  substates.contains(ANOMALY_DETECTOR) ? _kafkaCruiseControl.anomalyDetectorState() : null,
                                  _kafkaCruiseControl.config());
  }
}
