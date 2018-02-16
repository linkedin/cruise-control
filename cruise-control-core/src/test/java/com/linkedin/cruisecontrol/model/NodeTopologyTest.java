/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.model;

import com.linkedin.cruisecontrol.common.MockCluster;
import com.linkedin.cruisecontrol.common.MockLeafNodeImpl;
import com.linkedin.cruisecontrol.common.MockNodeImpl;
import com.linkedin.cruisecontrol.exception.ModelInputException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * Unit test for testing the basic topology functionality provided by a Node.
 */
@RunWith(Parameterized.class)
public class NodeTopologyTest {
  private static final Logger LOG = LoggerFactory.getLogger(NodeTopologyTest.class);
  private static final int MOCK_CLUSTER_CHILD_TIER = 4;
  enum Type { ADD, REMOVE }

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameters
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> params = new ArrayList<>();

    // Create a mock cluster.
    MockNodeImpl mockCluster = MockCluster.createMockCluster();

    // ****************************************************  ADD  ****************************************************

    // Test: Add a non-leaf node with non-conflicting id to an existing non-leaf node in a valid tier (No Exception)
    Node currentParent = mockCluster.child("r1");
    Object[] nonLeafChildAddParams = {Type.ADD, currentParent, 2, "h5", currentParent, null};
    params.add(nonLeafChildAddParams);

    // Test: Add a leaf node with non-conflicting id to an existing non-leaf node in a valid tier (No Exception)
    Node parentProcess = mockCluster.child("r1").child("h1").child("p1");
    Object[] leafChildAddParams = {Type.ADD, parentProcess, MOCK_CLUSTER_CHILD_TIER, "e5", parentProcess, null};
    params.add(leafChildAddParams);

    // Test: Add a non-leaf node with conflicting id to an existing non-leaf node (Exception)
    Object[] nonLeafChildConflictingIdAddParams = {Type.ADD, currentParent, 2, "h1", currentParent, IllegalStateException.class};
    params.add(nonLeafChildConflictingIdAddParams);

    // Test: Attempt to add a child to a non-leaf node whose original parent is different from its currentParent
    // -- i.e. moving a node in to a new parent node. (No Exception)
    Object[] moveOriginalToCurrentAddParams = {Type.ADD, mockCluster.child("r2"), 2, "h6", currentParent, null};
    params.add(moveOriginalToCurrentAddParams);

    // Test: Attempt to create a child to a non-leaf node whose tier is not one more than its currentParent (Exception)
    Object[] nonLeafChildTierMismatchAddParams = {Type.ADD, currentParent, 42, "h7", currentParent, ModelInputException.class};
    params.add(nonLeafChildTierMismatchAddParams);

    // Test: Attempt to add a child to a leaf node (Exception)
    Node parentLeaf = mockCluster.child("r1").child("h1").child("p1").child("e1");
    Object[] leafToLeafNodeAddParams = {Type.ADD, parentLeaf, MOCK_CLUSTER_CHILD_TIER + 1, "sub-e1", parentLeaf,
        IllegalStateException.class};
    params.add(leafToLeafNodeAddParams);

    // ***************************************************  REMOVE  ***************************************************

    // Test: Remove an existing leaf node from its currentParent (No Exception)
    Object[] leafChildRemoveParams = {Type.REMOVE, parentProcess, 4, "e5", parentProcess, null};
    params.add(leafChildRemoveParams);

    // Test: Remove a non-existing leaf node from an existing currentParent (Exception)
    Object[] nonExistingLeafChildRemoveParams = {Type.REMOVE, parentProcess, 4, "e50", parentProcess,
        IllegalStateException.class};
    params.add(nonExistingLeafChildRemoveParams);

    // Test: Remove an existing leaf node from an existing non-currentParent (Exception)
    Object[] existingLeafChildFromDifferentParentRemoveParams = {Type.REMOVE, parentProcess, 4, "e2", parentProcess,
        IllegalStateException.class};
    params.add(existingLeafChildFromDifferentParentRemoveParams);

    // Test: Remove an existing non-leaf node from its currentParent (No Exception)
    Object[] nonLeafChildRemoveParams = {Type.REMOVE, mockCluster, 1, "r2", mockCluster, null};
    params.add(nonLeafChildRemoveParams);

    return params;
  }
  private Type _type;
  private Node _currentParent;
  private int _childTier;
  private String _childId;
  private Node _originalParent;
  private Class<Throwable> _exceptionClass;

  public NodeTopologyTest(Type type, Node currentParent, int childTier, String childId, Node originalParent,
      Class<Throwable> exceptionClass) {
    _type = type;
    _currentParent = currentParent;
    _childTier = childTier;
    _childId = childId;
    _originalParent = originalParent;
    _exceptionClass = exceptionClass;
  }

  @Test
  public void testAdd() throws Exception {
    Assume.assumeTrue(_type == Type.ADD);
    LOG.debug("Testing node addition to topology with the mock cluster.");

    if (_exceptionClass != null) {
      expected.expect(_exceptionClass);
    }

    Node child;
    if (_childTier < MOCK_CLUSTER_CHILD_TIER) {
      child = new MockNodeImpl(_childTier, _childId, Collections.emptyMap(), _originalParent);
    } else {
      child = new MockLeafNodeImpl(_childTier, _childId, Collections.emptyMap(), _originalParent);
    }

    assertEquals("Previous current parent is different from the expected parent.",
        _currentParent.addChild(child), _originalParent.id());
    assertEquals("Parent does not contain the added child.", child, _currentParent.children().get(_childId));
    assertEquals("The current parent of child is not the new parent.", child.currentParent(), _currentParent);
  }

  @Test
  public void testRemove() throws Exception {
    Assume.assumeTrue(_type == Type.REMOVE);
    LOG.debug("Testing node removal from topology with the mock cluster.");

    if (_exceptionClass != null) {
      expected.expect(_exceptionClass);
    }
    String preRemoveCurrentParentId = _currentParent.id();
    String preRemoveOriginalParentId = _originalParent.id();
    Node child = _currentParent.removeChild(_childId);

    assertEquals("Removed child id is different from the expected id.", child.id(), _childId);
    assertEquals("Removed child tier is different from the expected tier.", child.tier(), _childTier);
    assertEquals("Original parent id change upon removal.", preRemoveOriginalParentId, child.originalParent().id());
    assertEquals("Current parent id change upon removal.", preRemoveCurrentParentId, child.currentParent().id());
    assertFalse("Parent still contains the removed child.", _currentParent.children().containsKey(_childId));
  }
}