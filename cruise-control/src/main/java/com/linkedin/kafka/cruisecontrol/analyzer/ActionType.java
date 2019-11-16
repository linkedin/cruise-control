/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Flags to indicate the type of an action.
 *
 * <ul>
 * <li>{@link #INTER_BROKER_REPLICA_MOVEMENT}: Move a replica from a source broker to a destination broker.</li>
 * <li>{@link #INTRA_BROKER_REPLICA_MOVEMENT}: Move a replica from a source disk to a destination disk.</li>
 * <li>{@link #LEADERSHIP_MOVEMENT}: Move leadership of a leader from a source broker to a follower of the same
 * partition residing in a destination broker.</li>
 * <li>{@link #REPLICA_ADDITION}: Add a new replica to the cluster.</li>
 * <li>{@link #REPLICA_DELETION}: Remove an existing replica from the cluster.</li>
 * <li>{@link #INTER_BROKER_REPLICA_SWAP}: Swap places of replicas residing in source and destination brokers.</li>
 * <li>{@link #INTRA_BROKER_REPLICA_SWAP}: Swap places of replicas residing in source and destination disks.</li>
 * </ul>
 */
public enum ActionType {
  INTER_BROKER_REPLICA_MOVEMENT("REPLICA"),
  INTRA_BROKER_REPLICA_MOVEMENT("INTRA_BROKER_REPLICA"),
  LEADERSHIP_MOVEMENT("LEADER"),
  REPLICA_ADDITION("ADDITION"),
  REPLICA_DELETION("DELETE"),
  INTER_BROKER_REPLICA_SWAP("SWAP"),
  INTRA_BROKER_REPLICA_SWAP("INTRA_BROKER_SWAP");

  private static final List<ActionType> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<ActionType> cachedValues() {
    return CACHED_VALUES;
  }

  private final String _balancingAction;

  ActionType(String balancingAction) {
    _balancingAction = balancingAction;
  }

  /**
   * @return Balancing action.
   */
  public String balancingAction() {
    return _balancingAction;
  }

  @Override
  public String toString() {
    return _balancingAction;
  }
}
