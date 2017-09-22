/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.exception;

public class CruiseControlMetricsReporterException extends Exception {

  public CruiseControlMetricsReporterException(String message, Throwable cause) {
    super(message, cause);
  }

  public CruiseControlMetricsReporterException(String message) {
    super(message);
  }

  public CruiseControlMetricsReporterException(Throwable cause) {
    super(cause);
  }
}
