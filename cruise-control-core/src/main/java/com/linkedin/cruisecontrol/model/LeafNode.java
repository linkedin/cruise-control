/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;


public interface LeafNode extends Node {

  /**
   * Add the given aggregatedMetricValues to the aggregatedMetricValues of this node and its ancestors.
   *
   * @param aggregatedMetricValuesToAdd Aggregated Metric Values to add to the aggregatedMetricValues of this node and
   *                                    its ancestors.
   */
  public void addAggregatedMetricValues(AggregatedMetricValues aggregatedMetricValuesToAdd);

  /**
   * Subtract the given aggregatedMetricValues from the aggregatedMetricValues of this node and its ancestors.
   *
   * @param aggregatedMetricValuesToSubtract Aggregated Metric Values to subtract from the aggregatedMetricValues of
   *                                         this node and its ancestors.
   */
  void subtractAggregatedMetricValues(AggregatedMetricValues aggregatedMetricValuesToSubtract);
}
