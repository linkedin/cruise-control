/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.rackaware;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;

/**
 * A transformer for mapping rack ID to different values.
 * <p>
 * This is to be used to customize rack Id interpretation for extra encoding, specifically should be used for rack aware goals when processing.
 * For example, if the broker encodes extra information into rack ID, but CC want to have the goals treating only a part of the rack ID as the
 * identifier of the rack, this mapper can be applied to extract the target part.
 */
public interface RackAwareGoalRackIdMapper extends CruiseControlConfigurable {
  /**
   * Transforms the input rackId
   *
   * @param rackId Rack ID to be transformed
   * @return Transformed rackId from raw rackId
   */
  String apply(String rackId);
}
