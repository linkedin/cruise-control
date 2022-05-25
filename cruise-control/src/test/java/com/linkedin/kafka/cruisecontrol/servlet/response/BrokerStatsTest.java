/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


public class BrokerStatsTest {
  @Test
  public void testBrokerStatsCoreNumPrinting() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    BrokerStats brokerStats = new BrokerStats(kafkaCruiseControlConfig);

    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0),
                                                 1.0);
    clusterModel.createRack("0");

    // Test broker with int cores (numCpuCores does not have fractional digits)
    BrokerCapacityInfo brokerCapacityInfoWithIntCores = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, 32.0);
    Broker intCoreBroker = clusterModel.createBroker("0", "0", 0, brokerCapacityInfoWithIntCores, false);
    brokerStats.addSingleBrokerStats(intCoreBroker, 0.0, true);
    assertTrue(brokerStats.toString().contains(",                 32,"));

    // Test broker with double cores (numCpuCores has fractional digits)
    BrokerCapacityInfo brokerCapacityInfoWithFractionalCores = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, 32.25111);
    Broker fractionalCoreBroker = clusterModel.createBroker("0", "0", 0, brokerCapacityInfoWithFractionalCores, false);
    brokerStats.addSingleBrokerStats(fractionalCoreBroker, 0.0, true);
    assertTrue(brokerStats.toString().contains(",              32.25,"));
  }
}
