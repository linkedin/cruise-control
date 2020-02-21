/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * The exception indicating something went wrong during the broker capacity resolving.
 */
public class BrokerCapacityResolvingException extends KafkaCruiseControlException {

  public BrokerCapacityResolvingException(String message, Throwable cause) {
    super(message, cause);
  }

  public BrokerCapacityResolvingException(String message) {
    super(message);
  }
}
