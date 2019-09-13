/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for execution proposals.
 */
public class ExecutionProposalTest {
  public static final TopicPartition TP = new TopicPartition("topic", 0);
  private final ReplicaPlacementInfo _r0 =  new ReplicaPlacementInfo(0);
  private final ReplicaPlacementInfo _r1 =  new ReplicaPlacementInfo(1);
  private final ReplicaPlacementInfo _r2 =  new ReplicaPlacementInfo(2);

  private final ReplicaPlacementInfo _r0d0 =  new ReplicaPlacementInfo(0, "tmp0");
  private final ReplicaPlacementInfo _r0d1 =  new ReplicaPlacementInfo(0, "tmp1");
  private final ReplicaPlacementInfo _r1d1 =  new ReplicaPlacementInfo(1, "tmp1");

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
    new ExecutionProposal(TP, 0, _r2, Arrays.asList(_r0, _r1), Arrays.asList(_r1, _r2));
  }

  @Test
  public void testIntraBrokerReplicaMovements() {
    ExecutionProposal p = new ExecutionProposal(TP, 0, _r0d0, Arrays.asList(_r0d0, _r1d1), Arrays.asList(_r0d1, _r1d1));
    Assert.assertEquals(1, p.replicasToMoveBetweenDisksByBroker().size());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testMingleReplicaMovements() {
    new ExecutionProposal(TP, 0, _r0d0, Arrays.asList(_r0d0, _r1), Arrays.asList(_r0d1, _r2));
  }
}
