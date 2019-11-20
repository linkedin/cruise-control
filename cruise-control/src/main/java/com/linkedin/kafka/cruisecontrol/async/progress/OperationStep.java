/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

/**
 * A single step of an operation.
 */
public interface OperationStep {

  /**
   * @return The name of the step.
   */
  String name();

  /**
   * @return The completion percentage of this step. The value should be between 0 and 1. Returning 1
   * means the step is completed.
   */
  float completionPercentage();

  /**
   * @return The description of this step.
   */
  String description();
}
