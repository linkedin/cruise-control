/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.Collections;
import java.util.List;


public enum Statistic {
  @JsonResponseField
  AVG("AVG"),
  @JsonResponseField
  MAX("MAX"),
  @JsonResponseField
  MIN("MIN"),
  @JsonResponseField
  ST_DEV("STD");

  private final String _stat;
  private static final List<Statistic> CACHED_VALUES = List.of(values());

  Statistic(String stat) {
    _stat = stat;
  }

  /**
   * @return The name of the statistic.
   */
  public String stat() {
    return _stat;
  }

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<Statistic> cachedValues() {
    return Collections.unmodifiableList(CACHED_VALUES);
  }

  @Override
  public String toString() {
    return _stat;
  }
}
