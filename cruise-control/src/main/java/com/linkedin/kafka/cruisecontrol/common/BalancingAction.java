/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.Arrays;
import java.util.List;


public enum BalancingAction {
  REPLICA_MOVEMENT("REPLICA"), 
  LEADERSHIP_MOVEMENT("LEADER"), 
  REPLICA_ADDITION("ADDITION"), 
  REPLICA_DELETION("DELETE");
  
  private static final List<BalancingAction> CACHED_VALUES;
  static {
    CACHED_VALUES = Arrays.asList(REPLICA_MOVEMENT, LEADERSHIP_MOVEMENT, REPLICA_ADDITION, REPLICA_DELETION);
  }
  
  public static List<BalancingAction> cachedValues() {
    return CACHED_VALUES;
  }

  private final String _balancingAction;

  BalancingAction(String balancingAction) {
    _balancingAction = balancingAction;
  }

  public String balancingAction() {
    return _balancingAction;
  }

  @Override
  public String toString() {
    return _balancingAction;
  }
}
