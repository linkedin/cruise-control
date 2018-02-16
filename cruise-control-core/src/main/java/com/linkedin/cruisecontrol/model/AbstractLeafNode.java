/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model;

import com.linkedin.cruisecontrol.exception.ModelInputException;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import java.util.Map;


public abstract class AbstractLeafNode extends AbstractNode implements LeafNode {

  public AbstractLeafNode(int tier, String id, Map<String, Object> tags, Node parent) throws ModelInputException {
    super(tier, id, tags, parent);
  }

  /**
   * Leaf nodes cannot have children.
   */
  @Override
  public String addChild(Node newChild) {
    throw new IllegalStateException(String.format("Leaf node: %s cannot have children.", id()));
  }

  /**
   * Leaf nodes cannot have children.
   */
  @Override
  public Node removeChild(String childId) {
    throw new IllegalStateException(String.format("Leaf node: %s cannot have children.", id()));
  }

  /**
   * Add the given aggregated metric values to the aggregated metric values of this node and its ancestors.
   *
   * @param aggregatedMetricValuesToAdd Aggregated metric values to add to the aggregated metric values of this node
   *                                    and its ancestors.
   */
  @Override
  public void addAggregatedMetricValues(AggregatedMetricValues aggregatedMetricValuesToAdd) {
    aggregatedMetricValues().add(aggregatedMetricValuesToAdd);
    for (Node ancestor = currentParent(); ancestor != null; ancestor = ancestor.currentParent()) {
      ancestor.aggregatedMetricValues().add(aggregatedMetricValuesToAdd);
    }
  }

  /**
   * Subtract the given aggregated metric values from the aggregated metric values of this node and its ancestors.
   *
   * @param aggregatedMetricValuesToSubtract Aggregated metric values to subtract from the aggregated metric values of
   *                                         this node and its ancestors.
   */
  @Override
  public void subtractAggregatedMetricValues(AggregatedMetricValues aggregatedMetricValuesToSubtract) {
    aggregatedMetricValues().subtract(aggregatedMetricValuesToSubtract);
    for (Node ancestor = currentParent(); ancestor != null; ancestor = ancestor.currentParent()) {
      ancestor.aggregatedMetricValues().subtract(aggregatedMetricValuesToSubtract);
    }
  }
}
