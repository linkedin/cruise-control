/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.exception;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregationOptions;

/**
 * Thrown when there is not enough valid windows to meet the requirements specified by {@link AggregationOptions}
 */
public class NotEnoughValidWindowsException extends CruiseControlException {
  public NotEnoughValidWindowsException(String msg) {
    super(msg);
  }
}
