/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model;

import com.linkedin.cruisecontrol.exception.ModelInputException;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public abstract class AbstractNode implements Node {

  private final int _tier;
  private final String _id;
  private final Map<String, Object> _tags;
  private final Set<String> _groupIds;
  private final AggregatedMetricValues _aggregatedMetricValues;
  private final Map<String, Node> _childById;
  private final Node _originalParent;
  private Node _currentParent;
  private boolean _isImmigrant;

  /**
   * Abstract node constructor.
   *
   * @param tier The tier of this node, representing its hierarchical position in the model.
   * @param id The id of this node.
   * @param tags Tags representing a map of key-values.
   * @param parent The hierarchical parent of this node.
   * @throws ModelInputException Attempt to initialize node with inconsistent topology information.
   */
  public AbstractNode(int tier, String id, Map<String, Object> tags, Node parent) throws ModelInputException {
    _tier = tier;
    _id = id;
    _tags = tags;

    if (parent == null) {
      // Sanity check: If currentParent is null, then the node should be the root node.
      if (_tier != 0) {
        throw new ModelInputException(String.format("The root node must have a tier of 0. Received: %d", _tier));
      }
    } else if (parent.tier() != _tier - 1) {
      // Sanity check for currentParent: Must satisfy (currentParent tier) == (this.tier - 1).
      throw new ModelInputException("Initialization failed due to inconsistent topology information. Parent tier "
          + "expected: " + (_tier - 1) + " received: " + parent.tier());
    }

    _groupIds = new HashSet<>();
    _aggregatedMetricValues = new AggregatedMetricValues();
    _childById = new HashMap<>();
    _originalParent = parent;
    _currentParent = parent;
    _isImmigrant = false;
  }

  /**
   * Get the tier of this node, representing its hierarchical position in the model.
   */
  @Override
  public int tier() {
    return _tier;
  }

  /**
   * Get the id of this node.
   */
  @Override
  public String id() {
    return _id;
  }

  /**
   * Get tag value of the node tag with the given key.
   *
   * @param key The key of the tag for which the value is requested.
   * @return The tag value of the node tag with the given key, or null if this tag contains no mapping for the key.
   */
  @Override
  public Object tagValue(String key) throws ModelInputException {
    if (key == null) {
      throw new ModelInputException("The key of the tag cannot be null.");
    }
    return _tags.get(key);
  }

  /**
   * Get tag ids of this node.
   */
  @Override
  public Set<String> tagIds() {
    return _tags.keySet();
  }

  /**
   * User-specified method called after updating the value of a tag with the given key from the given oldValue to its
   * current value.
   *
   * @param key The key of the tag that has been updated.
   * @param oldValue The value of the tag with the given key before the update.
   */
  protected abstract void onTagUpdate(String key, Object oldValue) throws ModelInputException;

  /**
   * Sanity check for the tag update with the given key for the proposed value.
   *
   * @param key The key of the tag proposed to be updated.
   * @param proposedValue The proposed value for the tag.
   * @throws ModelInputException The sanity check regarding the tag update has failed -- e.g. a tag cannot have a value
   * out of allowed options.
   */
  protected abstract void tagUpdateSanityCheck(String key, Object proposedValue) throws ModelInputException;

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
  @Override
  public Object updateTag(String key, Object proposedValue) throws ModelInputException {
    if (key == null) {
      throw new ModelInputException("The key of the tag cannot be null.");
    }
    tagUpdateSanityCheck(key, proposedValue);
    Object oldValue = _tags.put(key, proposedValue);
    onTagUpdate(key, oldValue);

    return oldValue;
  }

  /**
   * Set the current parent of this node. Update immigration status if necessary.
   *
   * @param currentParent The current parent of this node.
   * @return Previous currentParent.
   */
  @Override
  public String setCurrentParent(Node currentParent) throws ModelInputException {
    if (currentParent == null) {
      throw new ModelInputException("All non-root nodes must have a parent.");
    } else if (_tier == 0) {
      throw new ModelInputException(String.format("The root node: %s cannot have a parent.", _id));
    } else if (currentParent.tier() != _tier - 1) {
      throw new ModelInputException("Initialization failed due to inconsistent topology information. Parent tier "
          + "expected: " + (_tier - 1) + " received: " + currentParent.tier());
    }

    String prevCurrentParent = _currentParent.id();
    if (prevCurrentParent.equals(currentParent.id())) {
      // Do nothing if there is no change in the current parent.
      return prevCurrentParent;
    }

    _currentParent = currentParent;
    // Handle immigrant changes.
    if (!_originalParent.id().equals(currentParent.id())) {
      // If the current parent change indicates that the new parent is different from the original parent, this node
      // moved to a non-original node: This node and its descendants become immigrants.
      makeImmigrant();
    } else {
      // Back to the original node: If all of the ancestors of this node are non-immigrants, then
      // (1) This node becomes a non-immigrant, and
      // (2) Any descendant whose *all ancestors* have the original non-immigrant parent as their current parent,
      // becomes a non-immigrant.

      // Make this node non-immigrant if all its ancestors are (1) original, (2) non-immigrants.
      boolean isImmigrant = false;
      for (Node node = this; node != null; node = node.currentParent()) {
        if (node.currentParent().isImmigrant() || !node.originalParent().equals(node.currentParent())) {
          isImmigrant = true;
          break;
        }
      }
      if (!isImmigrant) {
        makeNonImmigrant();
      }
    }
    return prevCurrentParent;
  }

  /**
   * Get the current hierarchical parent of this node if exists, null if the node does not have a parent
   * -- i.e. the root node.
   */
  @Override
  public Node currentParent() {
    return _currentParent;
  }

  /**
   * Get the original hierarchical parent of this node if exists, null if the node does not have a parent
   * -- i.e. the root node.
   *
   * Original parent is the initial parent of this node.
   */
  @Override
  public Node originalParent() {
    return _originalParent;
  }

  /**
   * True if this node is immigrant, false otherwise. A node is immigrant if it moved away from its original location.
   */
  @Override
  public boolean isImmigrant() {
    return _isImmigrant;
  }

  /**
   * Make this node and all its descendants immigrant.
   */
  @Override
  public void makeImmigrant() throws ModelInputException {
    if (_currentParent == null) {
      throw new ModelInputException(String.format("Cannot migrate the root node: %s.", _id));
    }
    _isImmigrant = true;
    for (Node child: _childById.values()) {
      child.makeImmigrant();
    }
  }

  /**
   * Make this node and its descendants a non-immigrant node if all ancestors of such nodes have the original
   * non-immigrant parent as their current parent.
   *
   * Assumption: all ancestors of the node that calls this method must be non-immigrants.
   *
   * @throws ModelInputException Attempt to make the root node a non-immigrant.
   */
  @Override
  public void makeNonImmigrant() throws ModelInputException {
    // Make this node non-immigrant if all its ancestors are non-immigrants.
    if (_currentParent == null) {
      throw new ModelInputException(String.format("Cannot migrate the root node: %s.", _id));
    }

    _isImmigrant = _currentParent.isImmigrant() || !_currentParent.equals(_originalParent);
    // Any descendant whose all ancestors have the original, non-immigrant parent as their current parent, becomes a
    // non-immigrant.
    if (!_isImmigrant) {
      for (Node child: _childById.values()) {
        child.makeNonImmigrant();
      }
    }
  }

  /**
   * Get the set of ids for groups that this node is a member of.
   */
  @Override
  public Set<String> groupIds() {
    return _groupIds;
  }

  /**
   * Get the node that is (1) hierarchical child of this node and (2) has a unique id as childId.
   *
   * @param childId The unique id that identifies the hierarchical child of this node.
   * @return The child node with the given childId if exists, null otherwise.
   */
  @Override
  public Node child(String childId) throws ModelInputException {
    Node child = _childById.get(childId);
    if (child == null) {
      throw new ModelInputException(String.format("Child with the given id: %s does not exist.", childId));
    }
    return child;
  }

  /**
   * Get nodes that are hierarchical children of this node by their ids, an empty map if no children.
   */
  @Override
  public Map<String, Node> children() {
    return Collections.unmodifiableMap(_childById);
  }

  /**
   * Add the given node as a hierarchical child of this node. Ensure that (1) child tier = this node tier, and (2) the
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
  @Override
  public String addChild(Node newChild) throws ModelInputException {
    // Set the current parent of the child.
    String prevCurrentParent = newChild.setCurrentParent(this);

    // Add child if it has not already been added.
    if (_childById.putIfAbsent(newChild.id(), newChild) != null) {
      throw new IllegalStateException(String.format("Node: %s already has child: %s.", _id, newChild.id()));
    }

    // Increase the aggregatedMetricValues of this node and its ancestors by the aggregatedMetricValues of the new child.
    AggregatedMetricValues aggregatedMetricValuesToAdd = newChild.aggregatedMetricValues();
    _aggregatedMetricValues.add(aggregatedMetricValuesToAdd);
    for (Node ancestor = _currentParent; ancestor != null; ancestor = ancestor.currentParent()) {
      ancestor.aggregatedMetricValues().add(aggregatedMetricValuesToAdd);
    }

    return prevCurrentParent;
  }

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
  @Override
  public Node removeChild(String childId) {
    Node removedChild = _childById.remove(childId);
    if (removedChild == null) {
      throw new IllegalStateException(String.format("Inconsistent topology information. The node: %s does not have the "
          + "child: %s.", _id, childId));
    }

    // Decrease the aggregatedMetricValues of this node and its ancestors by the aggregatedMetricValues of the removed child.
    AggregatedMetricValues aggregatedMetricValuesToRemove = removedChild.aggregatedMetricValues();
    _aggregatedMetricValues.subtract(aggregatedMetricValuesToRemove);
    for (Node ancestor = _currentParent; ancestor != null; ancestor = ancestor.currentParent()) {
      ancestor.aggregatedMetricValues().subtract(aggregatedMetricValuesToRemove);
    }

    return removedChild;
  }

  /**
   * Add this node to the group with the given id.
   *
   * @param groupRegistry The group registry.
   * @param groupId Id of the group to which this node will be added.
   */
  @Override
  public void addToGroup(GroupRegistry groupRegistry, String groupId) {
    Group newGroup = groupRegistry.group(groupId);
    if (newGroup == null) {
      throw new IllegalStateException(String.format("Attempt to add node: %s to a non-existing group: %s.",
          _id, groupId));
    }

    newGroup.add(this);
    _groupIds.add(groupId);
  }

  /**
   * Remove this node from the group with the given id.
   *
   * @param groupRegistry The group registry.
   * @param groupId Id of the group for which this node will be removed.
   */
  @Override
  public void removeFromGroup(GroupRegistry groupRegistry, String groupId) {
    Group removedGroup = groupRegistry.group(groupId);
    if (removedGroup == null) {
      throw new IllegalStateException(String.format("Attempt to remove node: %s from a non-existing group: %s.",
          _id, groupId));
    }

    removedGroup.remove(this);
    _groupIds.remove(groupId);
  }

  /**
   * Get the node aggregatedMetricValues.
   */
  @Override
  public AggregatedMetricValues aggregatedMetricValues() {
    return _aggregatedMetricValues;
  }
}
