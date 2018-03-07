/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Arrays;
import java.util.List;


/**
 * Flags to indicate the type of an action.
 *
 * <ul>
 * <li>{@link #REPLICA_MOVEMENT}: Move a replica from a source broker to a destination broker.</li>
 * <li>{@link #LEADERSHIP_MOVEMENT}: Move leadership of a leader from a source broker to a follower of the same
 * partition residing in a destination broker.</li>
 * <li>{@link #REPLICA_ADDITION}: Add a new replica to the cluster.</li>
 * <li>{@link #REPLICA_DELETION}: Remove an existing replica from the cluster.</li>
 * <li>{@link #REPLICA_SWAP}: Swap places of replicas residing in source and destination brokers.</li>
 * </ul>
 */
public enum ActionType {
  REPLICA_MOVEMENT("REPLICA"),
  LEADERSHIP_MOVEMENT("LEADER"),
  REPLICA_ADDITION("ADDITION"),
  REPLICA_DELETION("DELETE"),
  REPLICA_SWAP("SWAP");

  private static final List<ActionType> CACHED_VALUES;
  static {
    CACHED_VALUES = Arrays.asList(REPLICA_MOVEMENT, LEADERSHIP_MOVEMENT, REPLICA_ADDITION, REPLICA_DELETION, REPLICA_SWAP);
  }

  public static List<ActionType> cachedValues() {
    return CACHED_VALUES;
  }

  private final String _balancingAction;

  ActionType(String balancingAction) {
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
