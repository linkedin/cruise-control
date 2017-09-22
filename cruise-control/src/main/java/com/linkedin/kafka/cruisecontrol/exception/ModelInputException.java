/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * Thrown if there is a problem with the model input.
 */
public class ModelInputException extends KafkaCruiseControlException {
  public ModelInputException(String message, Throwable cause) {
    super(message, cause);
  }

  public ModelInputException(String message) {
    super(message);
  }

  public ModelInputException(Throwable cause) {
    super(cause);
  }
}
