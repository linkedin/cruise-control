/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public enum Statistic {
  AVG("AVG"),
  MAX("MAX"),
  MIN("MIN"),
  ST_DEV("STD");

  private final String _stat;
  private static final List<Statistic> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  Statistic(String stat) {
    _stat = stat;
  }

  public String stat() {
    return _stat;
  }

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<Statistic> cachedValues() {
    return CACHED_VALUES;
  }

  @Override
  public String toString() {
    return _stat;
  }
}
