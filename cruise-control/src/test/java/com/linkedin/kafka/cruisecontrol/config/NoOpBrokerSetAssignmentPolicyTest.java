/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;


/**
 * Unit test for {@link NoOpBrokerSetAssignmentPolicy}
 */
public class NoOpBrokerSetAssignmentPolicyTest {
  private static final NoOpBrokerSetAssignmentPolicy NO_OP_BROKER_SET_ASSIGNMENT_POLICY = new NoOpBrokerSetAssignmentPolicy();

  /**
   * Tests if no op policy does not make any assignment to missing brokers
   */
  @Test
  public void testBrokerSetAssignment() throws BrokerSetResolutionException {
    final ClusterModel clusterModel = DeterministicCluster.brokerSetSatisfiable1();
    Map<String, Set<Integer>> mappedBrokers = Map.of("bs1", Set.of(0, 1, 2, 3, 4), "bs2", Set.of(5));
    Map<String, Set<Integer>> mappedBrokersAfterAssignment =
        NO_OP_BROKER_SET_ASSIGNMENT_POLICY.assignBrokerSetsForUnresolvedBrokers(clusterModel, mappedBrokers);

    assertNotNull(mappedBrokersAfterAssignment);

    assertEquals(mappedBrokersAfterAssignment, mappedBrokers);
  }

  /**
   * Tests if no op policy throws resolution exception when unassigned
   */
  @Test
  public void testBrokerSetAssignmentThrowsResolutionException() {
    final ClusterModel clusterModel = DeterministicCluster.brokerSetSatisfiable1();
    Map<String, Set<Integer>> mappedBrokers = Map.of("bs1", Set.of(0, 1, 2), "bs2", Set.of(5));
    assertThrows(BrokerSetResolutionException.class,
                 () -> NO_OP_BROKER_SET_ASSIGNMENT_POLICY.assignBrokerSetsForUnresolvedBrokers(clusterModel, mappedBrokers));
  }
}
