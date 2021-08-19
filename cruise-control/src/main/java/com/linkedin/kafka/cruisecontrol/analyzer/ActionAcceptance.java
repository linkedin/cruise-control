/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Collections;
import java.util.List;


/**
 * Flags to indicate if an action is acceptable by the goal(s).
 *
 * <ul>
 * <li>{@link #ACCEPT}: Action is acceptable -- i.e. it does not violate goal constraints.</li>
 * <li>{@link #REPLICA_REJECT}: Action is rejected in replica-level; but, the destination broker may potentially accept
 * actions of the same {@link ActionType} from the source broker specified in the given action.</li>
 * <li>{@link #BROKER_REJECT}: Action is rejected in broker-level; hence, the destination broker does not accept actions
 * of the same {@link ActionType} from the source broker specified in the given action.</li>
 * </ul>
 */
public enum ActionAcceptance {
  ACCEPT, REPLICA_REJECT, BROKER_REJECT;

  private static final List<ActionAcceptance> CACHED_VALUES = List.of(values());

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<ActionAcceptance> cachedValues() {
    return Collections.unmodifiableList(CACHED_VALUES);
  }
}
