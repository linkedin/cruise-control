/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Collections;
import java.util.List;


/**
 * Flags to indicate if the cluster is identified as under-provisioned, over-provisioned, or right-sized by the goal(s).
 * The goal can also be undecided if evaluation of a provision status is in progress or irrelevant to it.
 *
 * <ul>
 *   <li>{@link #UNDER_PROVISIONED}: Cluster is under-provisioned. Hence, it cannot satisfy the hard requirements set by the specified
 *   goal(s) (e.g. resource capacity goals), and needs more brokers to do so.</li>
 *   <li>{@link #RIGHT_SIZED}: The cluster is neither under-provisioned nor over-provisioned.</li>
 *   <li>{@link #OVER_PROVISIONED}: Cluster is over-provisioned. Hence, it is possible to remove brokers from the cluster while still
 *   satisfying its operational requirements set by the specified goal(s).</li>
 *   <li>{@link #UNDECIDED}: Evaluation of provision status is irrelevant or the goal has not decided on provision status, yet.</li>
 * </ul>
 */
public enum ProvisionStatus {
  UNDER_PROVISIONED, RIGHT_SIZED, OVER_PROVISIONED, UNDECIDED;

  private static final List<ProvisionStatus> CACHED_VALUES = List.of(values());

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<ProvisionStatus> cachedValues() {
    return Collections.unmodifiableList(CACHED_VALUES);
  }
}
