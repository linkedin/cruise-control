/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;

public class PrioritizeLargeMovementExecutionTaskStrategy extends AbstractExecutionTaskStrategy {

  @Override
  public int compareTask(ExecutionTask task1, ExecutionTask task2) {
    long dataToMoveForTask1 = task1.proposal().dataToMoveInMB();
    long dataToMoveForTask2 = task2.proposal().dataToMoveInMB();
    if (dataToMoveForTask2 != dataToMoveForTask1) {
      return (int) (dataToMoveForTask2 - dataToMoveForTask1);
    } else {
      return (int) (task1.executionId() - task2.executionId());
    }
  }
}