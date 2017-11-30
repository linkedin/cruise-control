/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model;

import com.linkedin.cruisecontrol.exception.ModelInputException;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.annotation.InterfaceStability;


@InterfaceStability.Evolving
public interface Node {

  /**
   * Get the tier of this node, representing its hierarchical position in the model.
   */
  int tier();

  /**
   * Get the id of this node.
   */
  String id();

  /**
   * Get tag value of the node tag with the given {@code key}.
   *
   * @param key The key of the tag for which the value is requested.
   * @return The tag value of the node tag with the given key, or null if this tag contains no mapping for the key.
   */
  Object tagValue(String key) throws ModelInputException;

  /**
   * Get tag ids of this node.
   */
  Set<String> tagIds();

  /**
   * Update the value of the tag with the given {@code key} to the given {@code proposedValue}
   * -- e.g. leader: true/false, throttle: true/false.
   *
   * Tag update might change:
   * (1) group membership of this node and its ancestors,
   * (2) aggregated metric values on a leaf node and its ancestors,
   * (3) aggregated metric values on aggregatedMetricValues-cacheable groups, and
   * (4) tags of the other nodes.
   *
   * Hence, the higher layer implementation should handle such changes in group membership and aggregated metric values.
   *
   * @param key The key of the tag for which the value will be updated.
   * @param proposedValue The proposed value of the tag with the given key.
   * @return Old value of the tag.
   * @throws ModelInputException The value for the tag with the given key is not allowed.
   */
  Object updateTag(String key, Object proposedValue) throws ModelInputException;

  /**
   * Set the current parent of this node. Update immigration status if necessary.
   *
   * @param currentParent The current parent of this node.
   * @return Previous currentParent.
   */
  String setCurrentParent(Node currentParent) throws ModelInputException;

  /**
   * Get the current hierarchical parent of this node if exists, null if the node does not have a parent
   * -- i.e. the root node.
   *
   * Current parent might be different from the original parent due to relocation of this node or its ancestors.
   */
  Node currentParent();

  /**
   * Get the original hierarchical parent of this node if exists, null if the node does not have a parent
   * -- i.e. the root node.
   *
   * Original parent is the initial parent of this node.
   */
  Node originalParent();

  /**
   * True if this node is immigrant, false otherwise. A node is immigrant if it moved away from its original location.
   */
  boolean isImmigrant();

  /**
   * Make this node and all its descendants immigrant.
   */
  void makeImmigrant() throws ModelInputException;

  /**
   * Make this node and its descendants a non-immigrant node if all ancestors of such nodes have the original
   * non-immigrant parent as their current parent.
   *
   * Assumption: all ancestors of the node that calls this method must be non-immigrants.
   *
   * @throws ModelInputException Attempt to make the root node a non-immigrant.
   */
  void makeNonImmigrant() throws ModelInputException;

  /**
   * Get the set of ids for groups that this node is a member of.
   */
  Set<String> groupIds();

  /**
   * Get the node that is (1) hierarchical child of this node and (2) has a unique id as {@code childId}.
   *
   * @param childId The unique id that identifies the hierarchical child of this node.
   * @return The child node with the given {@code childId} if exists, null otherwise.
   */
  Node child(String childId) throws ModelInputException;

  /**
   * Get nodes that are hierarchical children of this node by their ids, an empty map if no children.
   */
  public Map<String, Node> children();

  /**
   * Add the given node as a hierarchical child of this node. Ensure that (1) currentParent of child = this node, and (2) the
   * child has not already been added to this node. A new child affects the aggregatedMetricValues on this node.
   * A child may have its own children.
   *
   * An added (1) child, and (2) the descendants of the child may affect the corresponding group memberships (if any).
   * Hence, the higher layer implementation should handle such group membership changes.
   * This method does not modify group memberships.
   *
   * @param newChild Node to be added to this node as a child.
   * @return Previous currentParent.
   */
  String addChild(Node newChild) throws ModelInputException;

  /**
   * Remove and get the removed child node with given childId. A removed child affects the aggregatedMetricValues on
   * this node. A child may have its own children.
   *
   * A removed (1) child, and (2) the descendants of the child may affect the corresponding group memberships (if any).
   * Hence, the higher layer implementation should handle such group membership changes.
   * This method does not modify group memberships.
   *
   * @param childId The unique id that identifies the hierarchical child of this node.
   * @return The hierarchical child of the node with the given child id.
   */
  Node removeChild(String childId);

  /**
   * Add this node to the group with the given id.
   *
   * @param groupRegistry The group registry.
   * @param groupId Id of the group to which this node will be added.
   */
  void addToGroup(GroupRegistry groupRegistry, String groupId);

  /**
   * Remove this node from the group with the given id.
   *
   * @param groupRegistry The group registry.
   * @param groupId Id of the group for which this node will be removed.
   */
  void removeFromGroup(GroupRegistry groupRegistry, String groupId);

  /**
   * Get the node aggregatedMetricValues.
   */
  AggregatedMetricValues aggregatedMetricValues();
}