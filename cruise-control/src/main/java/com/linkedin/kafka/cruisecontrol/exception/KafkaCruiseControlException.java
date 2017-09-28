/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

import com.linkedin.cruisecontrol.exception.CruiseControlException;


/**
 * The parent exception for all the cruise control exceptions.
 */
public class KafkaCruiseControlException extends CruiseControlException {

  public KafkaCruiseControlException(String message, Throwable cause) {
    super(message, cause);
  }

  public KafkaCruiseControlException(String message) {
    super(message);
  }

  public KafkaCruiseControlException(Throwable cause) {
    super(cause);
  }

}
