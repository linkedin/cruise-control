/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.rackaware;

import java.util.Map;

/**
 * A no-op implementation of {@link RackAwareGoalRackIdMapper}
 */
public class NoOpRackAwareGoalRackIdMapper implements RackAwareGoalRackIdMapper {

  /**
   * A no-op transformation.  Returns input as-is.
   *
   * @param rackId Rack ID to be transformed
   * @return The input itself.
   */
  @Override
  public String apply(String rackId) {
    // No-OP
    return rackId;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // No-OP
  }

}
