/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.exception;

/**
 * Unknown version during Serialization/Deserialization.
 */
public class UnknownVersionException extends CruiseControlMetricsReporterException {
  public UnknownVersionException(String msg) {
    super(msg);
  }
}
