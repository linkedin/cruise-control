/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.Collections;
import java.util.List;


/**
 * Flags to indicate the type of a concurrency for an ongoing execution.
 *
 * <ul>
 *   <li>{@link #INTER_BROKER_REPLICA}: The concurrency of inter-broker replica reassignments.</li>
 *   <li>{@link #LEADERSHIP}: The concurrency of leadership movements.</li>
 *   <li>{@link #INTRA_BROKER_REPLICA}: The concurrency of intra-broker replica reassignments.</li>
 * </ul>
 */
public enum ConcurrencyType {
  @JsonResponseField
  INTER_BROKER_REPLICA,
  @JsonResponseField
  LEADERSHIP,
  @JsonResponseField
  INTRA_BROKER_REPLICA;

  private static final List<ConcurrencyType> CACHED_VALUES = List.of(values());

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<ConcurrencyType> cachedValues() {
    return Collections.unmodifiableList(CACHED_VALUES);
  }
}
