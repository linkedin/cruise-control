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
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import net.minidev.json.JSONArray;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlIntegrationTestUtils.KAFKA_CRUISE_CONTROL_BASE_PATH;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.KAFKA_CLUSTER_STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;

public class BrokerFailureIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final int PARTITION_COUNT = 10;
  private static final int KAFKA_CLUSTER_SIZE = 4;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + STATE + "?substates=analyzer&json=true";
  private final Configuration _gsonJsonConfig = KafkaCruiseControlIntegrationTestUtils.createJsonMappingConfig();

  private static final int BROKER_ID_TO_REMOVE = 1;

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
    configs.put(SelfHealingNotifier.BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG, "1000");
    configs.put(SelfHealingNotifier.BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG, "1500");
    configs.put(
        TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    configs.put(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaCapacityGoal.class.getName())
        .add(DiskCapacityGoal.class.getName()).toString());

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
  public void testBrokerFailure() {
    KafkaCruiseControlIntegrationTestUtils.createTopic(broker(0).plaintextAddr(), 
        new NewTopic(TOPIC0, PARTITION_COUNT, (short) 2));

    waitForMetadataPropagates();

    KafkaCruiseControlIntegrationTestUtils.produceRandomDataToTopic(TOPIC0, 4000, 
        KafkaCruiseControlIntegrationTestUtils.getDefaultProducerProperties(bootstrapServers()));

    waitForProposal();

    // shut down a broker to initiate a self-healing action
    broker(BROKER_ID_TO_REMOVE).shutdown();

    waitForSelfHealing();
  }

  private void waitForSelfHealing() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
        Integer brokers = JsonPath.<Integer>read(responseMessage, "KafkaBrokerState.Summary.Brokers");
        JSONArray partitionLeadersArray = JsonPath.read(responseMessage,
            "$.KafkaPartitionState.other[?(@.topic == '" + TOPIC0 + "')].leader");
        List<Integer> partitionLeaders = JsonPath.parse(partitionLeadersArray, _gsonJsonConfig)
            .read("$.*", new TypeRef<>() { });
        Map<String, String> offlineReplicas = JsonPath.read(responseMessage,
            "$.KafkaBrokerState.OfflineReplicaCountByBrokerId");
        return partitionLeaders.size() == PARTITION_COUNT && brokers == KAFKA_CLUSTER_SIZE - 1
            && offlineReplicas.isEmpty();
    }, 80, new AssertionError("Topic replicas not fixed after broker removed"));
  }

  private void waitForProposal() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_STATE_ENDPOINT);
      return JsonPath.<Boolean>read(responseMessage, "AnalyzerState.isProposalReady");
    }, 100, new AssertionError("No proposals were ready"));
  }

  private void waitForMetadataPropagates() {
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
