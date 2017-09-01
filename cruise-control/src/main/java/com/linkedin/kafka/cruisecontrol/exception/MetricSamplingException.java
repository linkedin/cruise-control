/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * The exception indicating something went wrong during the metrics sampling.
 */
public class MetricSamplingException extends KafkaCruiseControlException {

  public MetricSamplingException(String message) {
    super(message);
  }

}
