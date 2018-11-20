/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.util.Set;


/**
 * The async runnable for {@link KafkaCruiseControl#state(com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, Set)}
 */
class GetStateRunnable extends OperationRunnable {
  private final Set<CruiseControlState.SubState> _substates;

  GetStateRunnable(KafkaCruiseControl kafkaCruiseControl,
                   OperationFuture future,
                   CruiseControlStateParameters parameters) {
    super(kafkaCruiseControl, future);
    _substates = parameters.substates();
  }

  @Override
  protected CruiseControlState getResult() {
    return _kafkaCruiseControl.state(_future.operationProgress(), _substates);
  }
}
