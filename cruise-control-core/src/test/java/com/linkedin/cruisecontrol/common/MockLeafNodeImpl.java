/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common;

import com.linkedin.cruisecontrol.exception.ModelInputException;
import com.linkedin.cruisecontrol.model.AbstractLeafNode;
import com.linkedin.cruisecontrol.model.Node;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MockLeafNodeImpl extends AbstractLeafNode {
  private static final Logger LOG = LoggerFactory.getLogger(MockLeafNodeImpl.class);

  public MockLeafNodeImpl(int tier, String id, Map<String, Object> tags, Node parent) throws ModelInputException {
    super(tier, id, tags, parent);
  }

  /**
   * User-specified method called after updating the value of a tag with the given key from the given oldValue to its
   * current value.
   *  @param key The key of the tag that has been updated.
   * @param oldValue The value of the tag with the given key before the update.
   */
  @Override
  protected void onTagUpdate(String key, Object oldValue) throws ModelInputException {
    LOG.debug("User-specified method called after updating the tag value with the key: {} from the oldValue: {} to its "
        + "current value: {} in leaf node.", key, oldValue, tagValue(key));
  }

  /**
   * Sanity check for the tag update with the given key for the proposed value.
   *
   * @param key The key of the tag proposed to be updated.
   * @param proposedValue The proposed value for the tag.
   * @throws ModelInputException The sanity check regarding the tag update has failed -- e.g. a tag cannot have a value
   * out of allowed options.
   */
  @Override
  protected void tagUpdateSanityCheck(String key, Object proposedValue) throws ModelInputException {
    // 1. Check the validity of the given key.
    if (!tagIds().contains(key)) {
      throw new ModelInputException(String.format("The tag key: %s is invalid.", key));
    }

    // 2. Check the validity of a specific tag value.
    if (key.equals("answer") && !((Integer) proposedValue).equals(42)) {
      throw new ModelInputException(String.format("Attempt to set the tag value of the key: %s to a disallowed value: "
          + "%d.", key, (Integer) proposedValue));
    }
  }
}
