/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;


public class ProvisionerStateTest {
  @Test
  public void testProvisionerStateValidUpdate() {
    ProvisionerState.State originalState = ProvisionerState.State.COMPLETED;
    String originalSummary = "Test summary.";
    ProvisionerState provisionerState = new ProvisionerState(originalState, originalSummary);

    ProvisionerState.State updatedState = ProvisionerState.State.IN_PROGRESS;
    String updatedSummary = "Test summary.";
    provisionerState.update(updatedState, updatedSummary);

    assertEquals(updatedState, provisionerState.state());
    assertEquals(updatedSummary, provisionerState.summary());
  }

  @Test
  public void testProvisionerStateInvalidUpdateThrowsException() {
    ProvisionerState.State originalState = ProvisionerState.State.IN_PROGRESS;
    String originalSummary = "Test summary.";
    ProvisionerState provisionerState = new ProvisionerState(originalState, originalSummary);
    ProvisionerState.State updatedState = ProvisionerState.State.COMPLETED_WITH_ERROR;

    assertThrows(IllegalArgumentException.class, () -> provisionerState.update(originalState, null));
    assertThrows(IllegalStateException.class, () -> provisionerState.update(updatedState, originalSummary));
  }
}
