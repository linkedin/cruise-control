/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class OperationProgressTest {
  
  @Test
  public void testRefer() {
    OperationProgress progress1 = new OperationProgress();
    progress1.addStep(new Pending());
    OperationProgress progress2 = new OperationProgress();
    progress2.addStep(new WaitingForClusterModel());
    
    assertTrue(progress1.progress().get(0) instanceof Pending);
    progress1.refer(progress2);
    assertTrue(progress1.progress().get(0) instanceof WaitingForClusterModel);
    assertEquals(progress1.progress(), progress2.progress());
  }
  
  @Test
  public void testImmutableAfterRefer() {
    OperationProgress progress1 = new OperationProgress();
    OperationProgress progress2 = new OperationProgress();
    progress1.refer(progress2);
    
    try {
      progress1.addStep(new Pending());
      fail("Should have thrown IllegalStateException.");
    } catch (IllegalStateException ise) {
      // let it go.
    }

    try {
      progress1.refer(progress2);
      fail("Should have thrown IllegalStateException.");
    } catch (IllegalStateException ise) {
      // let it go.
    }

    try {
      progress1.clear();
      fail("Should have thrown IllegalStateException.");
    } catch (IllegalStateException ise) {
      // let it go.
    }
  }
}
