/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;

/**
 * The exception indicates that the broker with which the metric is associated could not be determined.
 */
public class InvalidPrometheusResultException extends KafkaCruiseControlException {

  public InvalidPrometheusResultException(String message) {
    super(message);
  }

}
