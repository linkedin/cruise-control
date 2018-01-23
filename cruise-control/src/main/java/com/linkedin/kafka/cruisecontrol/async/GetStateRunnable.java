/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;


/**
 * The async runnable for {@link KafkaCruiseControl#state(OperationProgress)}
 */
class GetStateRunnable extends OperationRunnable<KafkaCruiseControlState> {
  
  GetStateRunnable(KafkaCruiseControl kafkaCruiseControl, OperationFuture<KafkaCruiseControlState> future) {
    super(kafkaCruiseControl, future);
  }

  @Override
  protected KafkaCruiseControlState getResult() throws Exception {
    return _kafkaCruiseControl.state(_future.operationProgress());
  }
}
