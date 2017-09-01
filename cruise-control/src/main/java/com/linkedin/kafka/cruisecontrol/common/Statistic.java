/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

public enum Statistic {
  AVG("AVG"),
  MAX("MAX"),
  MIN("MIN"),
  ST_DEV("STD");

  private final String _stat;

  Statistic(String stat) {
    _stat = stat;
  }

  public String stat() {
    return _stat;
  }

  @Override
  public String toString() {
    return _stat;
  }
}
