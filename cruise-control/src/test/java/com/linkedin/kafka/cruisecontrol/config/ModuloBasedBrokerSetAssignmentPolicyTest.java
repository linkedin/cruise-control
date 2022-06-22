/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link ModuloBasedBrokerSetAssignmentPolicy}
 */
public class ModuloBasedBrokerSetAssignmentPolicyTest {
  private static final ModuloBasedBrokerSetAssignmentPolicy MODULO_BASED_BROKER_SET_ASSIGNMENT_POLICY =
      new ModuloBasedBrokerSetAssignmentPolicy();

  /**
   * Tests if modulo based broker set assignment policy makes assignments of unmapped brokers
   */
  @Test
  public void testBrokerSetAssignment() {
    // The cluster model has 6 brokers - 0,1,2,3,4,5
    final ClusterModel clusterModel = DeterministicCluster.brokerSetSatisfiable1();
    Map<String, Set<Integer>> mappedBrokers = new HashMap<>();
    mappedBrokers.put("bs1", new HashSet<>(Arrays.asList(0)));
    mappedBrokers.put("bs2", new HashSet<>(Arrays.asList(1)));

    Map<String, Set<Integer>> mappedBrokersAfterAssignment =
        MODULO_BASED_BROKER_SET_ASSIGNMENT_POLICY.assignBrokerSetsForUnresolvedBrokers(clusterModel, mappedBrokers);

    assertNotNull(mappedBrokersAfterAssignment);

    assertEquals(mappedBrokers.get("bs1"), Set.of(0, 2, 4));
    assertEquals(mappedBrokers.get("bs2"), Set.of(1, 3, 5));
  }
}
