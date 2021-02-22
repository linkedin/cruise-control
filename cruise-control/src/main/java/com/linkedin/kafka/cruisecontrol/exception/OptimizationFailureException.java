/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * An exception thrown when goal optimization failed.
 */
public class OptimizationFailureException extends KafkaCruiseControlException {
  private final String _recommendation;

  /**
   * @param message The detail message, which can be retrieved by the {@link #getMessage()}.
   */
  public OptimizationFailureException(String message) {
    this(message, "");
  }

  /**
   * @param message The detail message. The given recommendation will be appended to it, which can be retrieved by the {@link #getMessage()}.
   * @param recommendation Recommendation regarding the fix for this exception.
   */
  public OptimizationFailureException(String message, String recommendation) {
    super(String.format("%s %s", message, recommendation == null ? "" : recommendation));
    _recommendation = recommendation;
  }

  /**
   * @return Recommendation regarding the fix for this exception.
   */
  public String recommendation() {
    return _recommendation == null ? "" : _recommendation;
  }
}
