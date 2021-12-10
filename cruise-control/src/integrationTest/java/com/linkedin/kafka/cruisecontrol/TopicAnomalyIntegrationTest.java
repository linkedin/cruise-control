/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import net.minidev.json.JSONArray;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlIntegrationTestUtils.KAFKA_CRUISE_CONTROL_BASE_PATH;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.KAFKA_CLUSTER_STATE;

public class TopicAnomalyIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final int EXPECTED_REPLICA_COUNT = 3;
  private static final int PARTITION_COUNT = 2;
  private static final int KAFKA_CLUSTER_SIZE = 3;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private final Configuration _gsonJsonConfig = KafkaCruiseControlIntegrationTestUtils.createJsonMappingConfig();

  @Before
  public void setup() throws Exception {
    super.start();
  }

  @Override
  protected int clusterSize() {
    return KAFKA_CLUSTER_SIZE;
  }

  @After
  public void teardown() {
    super.stop();
  }

  @Override
  public Map<Object, Object> overridingProps() {
    return KafkaCruiseControlIntegrationTestUtils.createBrokerProps();
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = KafkaCruiseControlIntegrationTestUtils.ccConfigOverrides();
    configs.put(TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, "3");
    configs.put(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaCapacityGoal.class.getName())
        .add(DiskCapacityGoal.class.getName())
        .add(ReplicaDistributionGoal.class.getName()).toString());

    configs.put(AnalyzerConfig.DEFAULT_GOALS_CONFIG, new StringJoiner(",")
        .add(MinTopicLeadersPerBrokerGoal.class.getName())
        .add(ReplicaCapacityGoal.class.getName())
        .add(ReplicaDistributionGoal.class.getName()).toString());

    configs.put(AnalyzerConfig.HARD_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaCapacityGoal.class.getName())
        .add(MinTopicLeadersPerBrokerGoal.class.getName()).toString());

    return configs;
  }

  @Test
  public void testTopicAnomalyFinder() {
    KafkaCruiseControlIntegrationTestUtils.createTopic(broker(0).plaintextAddr(),
        new NewTopic(TOPIC0, PARTITION_COUNT, (short) 2));

    waitForMetadataPropogates();

    KafkaCruiseControlIntegrationTestUtils.produceRandomDataToTopic(TOPIC0, 4000,
        KafkaCruiseControlIntegrationTestUtils.getDefaultProducerProperties(bootstrapServers()));

    waitUntilNewReplicasAppearForTheTopic();

  }

  private void waitUntilNewReplicasAppearForTheTopic() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
      JSONArray replicasArray = JsonPath.read(responseMessage,
          "$.KafkaPartitionState.other[?(@.topic == '" + TOPIC0 + "')].replicas");
      List<List<Integer>> partitionReplicas = JsonPath.parse(replicasArray, _gsonJsonConfig)
          .read("$.*", new TypeRef<>() { });
      return partitionReplicas.stream().allMatch(i -> i.size() == EXPECTED_REPLICA_COUNT);
    }, 100, new AssertionError("Replica count not match"));
  }

  private void waitForMetadataPropogates() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
        String responseMessage = KafkaCruiseControlIntegrationTestUtils
            .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
        JSONArray partitionLeadersArray = JsonPath.read(responseMessage,
            "$.KafkaPartitionState.other[?(@.topic == '" + TOPIC0 + "')].leader");
        List<Integer> partitionLeaders = JsonPath.parse(partitionLeadersArray, _gsonJsonConfig)
            .read("$.*", new TypeRef<>() { });
        return partitionLeaders.size() == PARTITION_COUNT;
    }, 80, new AssertionError("Topic partitions not found for " + TOPIC0));
  }

}
