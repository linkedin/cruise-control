/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.model;

import com.linkedin.cruisecontrol.common.MockCluster;
import com.linkedin.cruisecontrol.common.MockNodeImpl;
import com.linkedin.cruisecontrol.exception.ModelInputException;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;


/**
 * Unit test for testing the tag set and update functionality provided by a Node.
 */
@RunWith(Parameterized.class)
public class NodeTagTest {
  private static final Logger LOG = LoggerFactory.getLogger(NodeTagTest.class);

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameters
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> params = new ArrayList<>();

    // Create a mock cluster.
    MockNodeImpl mockCluster = MockCluster.createMockCluster();

    // Test: Update an existing tag value of a node to an allowed value (No Exception)
    Object[] allowedTagUpdateParams = {mockCluster, "location", "Sunnyvale", "San Francisco", null, false};
    params.add(allowedTagUpdateParams);

    // Test: Attempt to update tag value of a node to a disallowed value (Exception)
    Node aLeafNode = mockCluster.child("r1").child("h1").child("p1").child("e1");
    Object[] disallowedTagUpdateParams = {aLeafNode, "answer", 1, -42, ModelInputException.class, false};
    params.add(disallowedTagUpdateParams);

    // Test: Attempt to get a tag with a null key (Exception)
    Object[] nullKeyGetParams = {mockCluster, null, null, "", ModelInputException.class, true};
    params.add(nullKeyGetParams);

    // Test: Attempt to update a tag with a null key (Exception)
    Object[] nullKeyUpdateParams = {mockCluster, null, null, "", ModelInputException.class, false};
    params.add(nullKeyUpdateParams);

    // Test: Set a non-existing tag value of a node to an allowed value (Exception)
    Object[] nonExistingTagParams = {mockCluster, "aNonExistingKey", null, "aNewValue", ModelInputException.class, false};
    params.add(nonExistingTagParams);

    return params;
  }

  private Node _node;
  private String _key;
  private Object _expectedInitTag;
  private Object _proposedValue;
  private Class<Throwable> _exceptionClass;
  private boolean _isGetTagOnly;

  public NodeTagTest(Node node, String key, Object expectedInitTag, Object proposedValue,
                     Class<Throwable> exceptionClass, boolean isGetTagOnly) {
    _node = node;
    _key = key;
    _expectedInitTag = expectedInitTag;
    _proposedValue = proposedValue;
    _exceptionClass = exceptionClass;
    _isGetTagOnly = isGetTagOnly;
  }

  @Test
  public void testTag() throws Exception {
    LOG.debug("Testing tag update with the mock cluster.");
    if (_exceptionClass != null) {
      expected.expect(_exceptionClass);
    }

    if (_isGetTagOnly) {
      assertEquals("Initial tag value mismatch with the expected value.", _expectedInitTag, _node.tagValue(_key));
    } else {
      assertEquals("Tag update failed to return the old value.", _expectedInitTag, _node.updateTag(_key, _proposedValue));
      assertEquals("Tag update failed to set to the new value.", _node.tagValue(_key), _proposedValue);
    }
  }
}