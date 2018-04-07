/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.exception;

public class CruiseControlException extends Exception {

  public CruiseControlException(String message, Throwable cause) {
    super(message, cause);
  }

  public CruiseControlException(String message) {
    super(message);
  }

  public CruiseControlException(Throwable cause) {
    super(cause);
  }
}
