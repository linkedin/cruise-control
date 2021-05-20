/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Collections;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.Test;


public class MaintenancePlanSerdeTest {
  private static final int TEST_BROKER_ID = 42;
  private static final String TOPIC_NAME = "TestTopic";
  private static final SortedSet<Integer> BROKERS_IN_PLAN;
  static {
    SortedSet<Integer> brokersInPlan = new TreeSet<>();
    brokersInPlan.add(42);
    brokersInPlan.add(24);
    BROKERS_IN_PLAN = Collections.unmodifiableSortedSet(brokersInPlan);
  }

  private static final SortedMap<Short, String> TEST_TOPIC_REGEX_WITH_RF_UPDATE;
  static {
    SortedMap<Short, String> testTopicRegexWithRfUpdate = new TreeMap<>();
    testTopicRegexWithRfUpdate.put((short) 2, "T2");
    testTopicRegexWithRfUpdate.put((short) 3, "T3");
    TEST_TOPIC_REGEX_WITH_RF_UPDATE = Collections.unmodifiableSortedMap(testTopicRegexWithRfUpdate);
  }

  @Test
  public void testDemotePlanSerde() {
    MaintenancePlanSerde maintenancePlanSerde = new MaintenancePlanSerde();
    DemoteBrokerPlan demoteBrokerPlanA = new DemoteBrokerPlan(System.currentTimeMillis(), TEST_BROKER_ID, BROKERS_IN_PLAN);
    byte[] serializedJson = maintenancePlanSerde.serialize(TOPIC_NAME, demoteBrokerPlanA);
    DemoteBrokerPlan demoteBrokerPlanB = (DemoteBrokerPlan) maintenancePlanSerde.deserialize(TOPIC_NAME, serializedJson);
    Assert.assertNotNull(demoteBrokerPlanB);
    Assert.assertEquals(demoteBrokerPlanA.getCrc(), demoteBrokerPlanB.getCrc());
  }

  @Test
  public void testAddBrokerPlanSerde() {
    MaintenancePlanSerde maintenancePlanSerde = new MaintenancePlanSerde();
    AddBrokerPlan addBrokerPlanA = new AddBrokerPlan(System.currentTimeMillis(), TEST_BROKER_ID, BROKERS_IN_PLAN);
    byte[] serializedJson = maintenancePlanSerde.serialize(TOPIC_NAME, addBrokerPlanA);
    AddBrokerPlan addBrokerPlanB = (AddBrokerPlan) maintenancePlanSerde.deserialize(TOPIC_NAME, serializedJson);
    Assert.assertNotNull(addBrokerPlanB);
    Assert.assertEquals(addBrokerPlanA.getCrc(), addBrokerPlanB.getCrc());
  }

  @Test
  public void testRemoveBrokerPlanSerde() {
    MaintenancePlanSerde maintenancePlanSerde = new MaintenancePlanSerde();
    RemoveBrokerPlan removeBrokerPlanA = new RemoveBrokerPlan(System.currentTimeMillis(), TEST_BROKER_ID, BROKERS_IN_PLAN);
    byte[] serializedJson = maintenancePlanSerde.serialize(TOPIC_NAME, removeBrokerPlanA);
    RemoveBrokerPlan removeBrokerPlanB = (RemoveBrokerPlan) maintenancePlanSerde.deserialize(TOPIC_NAME, serializedJson);
    Assert.assertNotNull(removeBrokerPlanB);
    Assert.assertEquals(removeBrokerPlanA.getCrc(), removeBrokerPlanB.getCrc());
  }

  @Test
  public void testFixOfflineReplicasPlanSerde() {
    MaintenancePlanSerde maintenancePlanSerde = new MaintenancePlanSerde();
    FixOfflineReplicasPlan fixOfflineReplicasPlanA = new FixOfflineReplicasPlan(System.currentTimeMillis(), TEST_BROKER_ID);
    byte[] serializedJson = maintenancePlanSerde.serialize(TOPIC_NAME, fixOfflineReplicasPlanA);
    FixOfflineReplicasPlan fixOfflineReplicasPlanB = (FixOfflineReplicasPlan) maintenancePlanSerde.deserialize(TOPIC_NAME, serializedJson);
    Assert.assertNotNull(fixOfflineReplicasPlanB);
    Assert.assertEquals(fixOfflineReplicasPlanA.getCrc(), fixOfflineReplicasPlanB.getCrc());
  }

  @Test
  public void testRebalancePlanSerde() {
    MaintenancePlanSerde maintenancePlanSerde = new MaintenancePlanSerde();
    RebalancePlan rebalancePlanA = new RebalancePlan(System.currentTimeMillis(), TEST_BROKER_ID);
    byte[] serializedJson = maintenancePlanSerde.serialize(TOPIC_NAME, rebalancePlanA);
    RebalancePlan rebalancePlanB = (RebalancePlan) maintenancePlanSerde.deserialize(TOPIC_NAME, serializedJson);
    Assert.assertNotNull(rebalancePlanB);
    Assert.assertEquals(rebalancePlanA.getCrc(), rebalancePlanB.getCrc());
  }

  @Test
  public void testTopicReplicationFactorPlanSerde() {
    MaintenancePlanSerde maintenancePlanSerde = new MaintenancePlanSerde();
    TopicReplicationFactorPlan topicReplicationFactorPlanA =
        new TopicReplicationFactorPlan(System.currentTimeMillis(), TEST_BROKER_ID, TEST_TOPIC_REGEX_WITH_RF_UPDATE);
    byte[] serializedJson = maintenancePlanSerde.serialize(TOPIC_NAME, topicReplicationFactorPlanA);
    TopicReplicationFactorPlan topicReplicationFactorPlanB =
        (TopicReplicationFactorPlan) maintenancePlanSerde.deserialize(TOPIC_NAME, serializedJson);
    Assert.assertNotNull(topicReplicationFactorPlanB);
    Assert.assertEquals(topicReplicationFactorPlanA.getCrc(), topicReplicationFactorPlanB.getCrc());
  }
}
