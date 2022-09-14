/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Map;
import java.util.Set;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;


/**
 * Unit test for {@link BrokerSetResolutionHelper}
 */
public class BrokerSetResolutionHelperTest {

  /**
   * Tests the happy path for broker set resolution helper class
   * @throws BrokerSetResolutionException
   */
  @Test
  public void testBrokerSetResolutionHelper() throws BrokerSetResolutionException {
    ClusterModel clusterModel = DeterministicCluster.brokerSetSatisfiable1();
    Map<String, Set<Integer>> testBrokerSetMapping = Map.of("BS1", Set.of(0), "BS2", Set.of(1, 2), "BS3", Set.of(3, 4), "BS4", Set.of(5));

    BrokerSetResolver brokerSetResolver = EasyMock.createNiceMock(BrokerSetResolver.class);
    EasyMock.expect(brokerSetResolver.brokerIdsByBrokerSetId(BrokerSetResolutionHelper.getRackIdByBrokerIdMapping(clusterModel)))
            .andReturn(testBrokerSetMapping);
    EasyMock.replay(brokerSetResolver);

    BrokerSetResolutionHelper brokerSetResolutionHelper = new BrokerSetResolutionHelper(clusterModel, brokerSetResolver);

    assertEquals("BS1", brokerSetResolutionHelper.brokerSetId(0));
    assertEquals("BS2", brokerSetResolutionHelper.brokerSetId(1));
    assertEquals("BS2", brokerSetResolutionHelper.brokerSetId(2));
    assertEquals("BS3", brokerSetResolutionHelper.brokerSetId(3));
    assertEquals("BS3", brokerSetResolutionHelper.brokerSetId(4));
    assertEquals("BS4", brokerSetResolutionHelper.brokerSetId(5));
  }

  /**
   * Tests case where a broker is missing mapping with default configurations
   * @throws BrokerSetResolutionException
   */
  @Test
  public void testBrokerSetResolutionHelperMissingBroker() throws BrokerSetResolutionException {
    ClusterModel clusterModel = DeterministicCluster.brokerSetSatisfiable1();
    Map<String, Set<Integer>> testBrokerSetMapping = Map.of("BS1", Set.of(0), "BS2", Set.of(1, 2), "BS3", Set.of(3, 4));

    BrokerSetResolver brokerSetResolver = EasyMock.createNiceMock(BrokerSetResolver.class);
    EasyMock.expect(brokerSetResolver.brokerIdsByBrokerSetId(BrokerSetResolutionHelper.getRackIdByBrokerIdMapping(clusterModel)))
            .andReturn(testBrokerSetMapping);
    EasyMock.replay(brokerSetResolver);

    BrokerSetResolutionHelper brokerSetResolutionHelper = new BrokerSetResolutionHelper(clusterModel, brokerSetResolver);

    assertEquals("BS1", brokerSetResolutionHelper.brokerSetId(0));
    assertEquals("BS2", brokerSetResolutionHelper.brokerSetId(1));
    assertEquals("BS2", brokerSetResolutionHelper.brokerSetId(2));
    assertEquals("BS3", brokerSetResolutionHelper.brokerSetId(3));
    assertEquals("BS3", brokerSetResolutionHelper.brokerSetId(4));
    assertThrows(BrokerSetResolutionException.class, () -> brokerSetResolutionHelper.brokerSetId(5));
  }
}

