/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import java.util.Set;


/**
 * The async runnable for {@link KafkaCruiseControl#state(OperationProgress, Set)}
 */
class GetStateRunnable extends OperationRunnable<KafkaCruiseControlState> {
  private final Set<KafkaCruiseControlState.SubState> _substates;

  GetStateRunnable(KafkaCruiseControl kafkaCruiseControl,
                   OperationFuture<KafkaCruiseControlState> future,
                   Set<KafkaCruiseControlState.SubState> substates) {
    super(kafkaCruiseControl, future);
    _substates = substates;
  }

  @Override
  protected KafkaCruiseControlState getResult() {
    return _kafkaCruiseControl.state(_future.operationProgress(), _substates);
  }
}
