/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;


/**
 * Unit test for execution proposals.
 */
public class ExecutionProposalTest {
  private final TopicPartition TP = new TopicPartition("topic", 0);

  @Test (expected = IllegalArgumentException.class)
  public void testNullNewReplicaList() {
    new ExecutionProposal(TP, 0, 1, Arrays.asList(0, 1), null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testEmptyNewReplicaList() {
    new ExecutionProposal(TP, 0, 1, Arrays.asList(0, 1), Collections.emptyList());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDuplicateReplicaInNewReplicaList() {
    new ExecutionProposal(TP, 0, 1, Arrays.asList(0, 1), Arrays.asList(2, 2));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testOldLeaderMissingFromOldReplicas() {
    new ExecutionProposal(TP, 0, 2, Arrays.asList(0, 1), Arrays.asList(2, 2));
  }
}
