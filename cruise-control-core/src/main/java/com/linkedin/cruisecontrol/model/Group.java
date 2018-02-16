/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * A group is a bundle of nodes.
 */
public class Group {

  private final String _id;
  private final boolean _isAggregatedMetricValuesCacheable;
  private final Map<String, Node> _nodes;
  private final AggregatedMetricValues _aggregatedMetricValues;

  public Group(String id, boolean isAggregatedMetricValuesCacheable) {
    _id = id;
    _isAggregatedMetricValuesCacheable = isAggregatedMetricValuesCacheable;
    _nodes = new HashMap<>();
    _aggregatedMetricValues = isAggregatedMetricValuesCacheable ? new AggregatedMetricValues() : null;
  }

  /**
   * Get the unique id of the group.
   */
  public String id() {
    return _id;
  }

  /**
   * Get whether the aggregatedMetricValues of group is cacheable.
   */
  public boolean isAggregatedMetricValuesCacheable() {
    return _isAggregatedMetricValuesCacheable;
  }

  /**
   * Get a map of group nodes of the group by their id.
   */
  public Map<String, Node> nodes() {
    return Collections.unmodifiableMap(_nodes);
  }

  /**
   * Get cached aggregatedMetricValues of the group, or null if the aggregatedMetricValues is not cacheable.
   */
  public AggregatedMetricValues aggregatedMetricValues() {
    return _aggregatedMetricValues;
  }

  /**
   * Get the node that is a member of this group, having the given nodeId.
   *
   * @param nodeId The unique id of the node residing in the group.
   * @return The node that is a member of the group having the given nodeId.
   */
  public Node nodeFor(String nodeId) {
    return _nodes.get(nodeId);
  }

  /**
   * Add node to the group.
   *
   * @param nodeToAdd Node to add to this group.
   */
  protected void add(Node nodeToAdd) {
    _nodes.put(nodeToAdd.id(), nodeToAdd);
    if (_isAggregatedMetricValuesCacheable) {
      _aggregatedMetricValues.add(nodeToAdd.aggregatedMetricValues());
    }
  }

  /**
   * Remove node from the group.
   *
   * @param nodeToRemove Node to remove from this group.
   */
  protected void remove(Node nodeToRemove) {
    _nodes.remove(nodeToRemove.id());
    if (_isAggregatedMetricValuesCacheable) {
      _aggregatedMetricValues.subtract(nodeToRemove.aggregatedMetricValues());
    }
  }

  /**
   * Clear the nodes and aggregatedMetricValues (if aggregatedMetricValues cacheable) in this group.
   */
  protected void clear() {
    _nodes.clear();
    if (_isAggregatedMetricValuesCacheable) {
      _aggregatedMetricValues.clear();
    }
  }
}
