/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;


public class BaseExecutionTaskStrategy extends AbstractExecutionTaskStrategy {

  @Override
  public int compareTask(ExecutionTask task1, ExecutionTask task2) {
    return (int) (task1.executionId() - task2.executionId());
  }
}
