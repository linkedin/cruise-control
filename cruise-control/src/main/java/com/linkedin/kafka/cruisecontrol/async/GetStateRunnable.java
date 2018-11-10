/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.util.Set;


/**
 * The async runnable for {@link KafkaCruiseControl#state(com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress,
 * Set, com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager)}
 */
class GetStateRunnable extends OperationRunnable {
  private final Set<KafkaCruiseControlState.SubState> _substates;
  private final UserTaskManager _userTaskManager;

  GetStateRunnable(KafkaCruiseControl kafkaCruiseControl,
                   OperationFuture future,
                   CruiseControlStateParameters parameters,
                   UserTaskManager userTaskManager) {
    super(kafkaCruiseControl, future);
    _substates = parameters.substates();
    _userTaskManager = userTaskManager;
  }

  @Override
  protected KafkaCruiseControlState getResult() {
    return _kafkaCruiseControl.state(_future.operationProgress(), _substates, _userTaskManager);
  }
}
