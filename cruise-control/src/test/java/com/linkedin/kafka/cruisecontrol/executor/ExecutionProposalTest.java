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
  private static final TopicPartition TP = new TopicPartition("topic", 0);
  private final Integer _r0 =  0;
  private final Integer _r1 =  1;
  private final Integer _r2 =  2;

  @Test (expected = IllegalArgumentException.class)
  public void testNullNewReplicaList() {
    new ExecutionProposal(TP, 0, _r1, Arrays.asList(_r0, _r1), null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testEmptyNewReplicaList() {
    new ExecutionProposal(TP, 0, _r1, Arrays.asList(_r0, _r1), Collections.emptyList());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDuplicateReplicaInNewReplicaList() {
    new ExecutionProposal(TP, 0, _r1, Arrays.asList(_r0, _r1), Arrays.asList(_r2, _r2));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testOldLeaderMissingFromOldReplicas() {
    new ExecutionProposal(TP, 0, _r2, Arrays.asList(_r0, _r1), Arrays.asList(_r2, _r2));
  }
}
