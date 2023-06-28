/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.rackaware.RackAwareGoalRackIdMapper;
import java.util.Map;


public class IgnorePrefixRackIdMapper implements RackAwareGoalRackIdMapper {
  /**
   * A simple transformer that returns everything after the first "::"
   * For example, "::123", "123", "AA::123" would all return 123, and "A::B::123" would return "B::123"
   */
  @Override
  public String apply(String rackId) {
    final String[] split = rackId.split("::", 2);
    return split[split.length - 1];
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // No-OP
  }
}
