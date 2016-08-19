/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * Thrown if an analysis input is missing (null).
 */
public class AnalysisInputException extends KafkaCruiseControlException {

  public AnalysisInputException(String message, Throwable cause) {
    super(message, cause);
  }

  public AnalysisInputException(String message) {
    super(message);
  }

  public AnalysisInputException(Throwable cause) {
    super(cause);
  }
}