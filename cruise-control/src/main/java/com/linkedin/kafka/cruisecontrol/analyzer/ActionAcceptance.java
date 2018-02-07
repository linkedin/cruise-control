/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Arrays;
import java.util.List;


public enum ActionAcceptance {
  ACCEPT("ACCEPT"),
  REPLICA_REJECT("REPLICA_REJECT"),
  BROKER_REJECT("BROKER_REJECT");

  private static final List<ActionAcceptance> CACHED_VALUES;
  static {
    CACHED_VALUES = Arrays.asList(ACCEPT, REPLICA_REJECT, BROKER_REJECT);
  }

  public static List<ActionAcceptance> cachedValues() {
    return CACHED_VALUES;
  }

  private final String _actionAcceptance;

  ActionAcceptance(String actionAcceptance) {
    _actionAcceptance = actionAcceptance;
  }

  public String actionAcceptance() {
    return _actionAcceptance;
  }

  @Override
  public String toString() {
    return _actionAcceptance;
  }
}
