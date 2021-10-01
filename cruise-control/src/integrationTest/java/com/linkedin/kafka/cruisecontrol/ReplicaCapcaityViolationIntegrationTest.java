/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See
 * License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedBroker;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import net.minidev.json.JSONArray;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.KAFKA_CLUSTER_STATE;


public class ReplicaCapcaityViolationIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final int BROKER_ID_TO_ADD = 3;
  private static final int PARTITION_COUNT = 2;
  private static final int KAFKA_CLUSTER_SIZE = 3;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      "kafkacruisecontrol/" + KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT =
      "kafkacruisecontrol/" + CruiseControlEndPoint.STATE + "?substates=anomaly_detector&json=true";
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
    Map<Object, Object> props = KafkaCruiseControlIntegrationTestUtils.createBrokerProps();
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "15000");
    return props;
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = KafkaCruiseControlIntegrationTestUtils.ccConfigOverrides();
    configs.put(TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    
    configs.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "15000");
    configs.put(MonitorConfig.METADATA_MAX_AGE_MS_CONFIG, "15000");
    configs.put(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG, "20000");
    configs.put(AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG, "20000");
    
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
    
    configs.put(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, "4");
    configs.put(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, "4");
    return configs;
  }

  @Test
  public void testReplicaCapacityViolation() throws InterruptedException, ExecutionException {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections
        .singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC0, PARTITION_COUNT, (short) 2))).all().get();

    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }

    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
        String responseMessage = KafkaCruiseControlIntegrationTestUtils
            .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_STATE_ENDPOINT);
        JSONArray unfixableGoalsArray = JsonPath.<JSONArray>read(responseMessage,
            "$.AnomalyDetectorState.recentGoalViolations[*].unfixableViolatedGoals.[*]");
        List<String> unfixableGoals = JsonPath.parse(unfixableGoalsArray, _gsonJsonConfig)
            .read("$..*", new TypeRef<List<String>>() { });
        return unfixableGoals.contains("ReplicaCapacityGoal");
    }, 90, new AssertionError("Replica capacity goal violation not found"));

    Map<Object, Object> createBrokerConfig = createBrokerConfig(BROKER_ID_TO_ADD);
    CCEmbeddedBroker broker = new CCEmbeddedBroker(createBrokerConfig);
    broker.startup();
    
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
        String responseMessage = KafkaCruiseControlIntegrationTestUtils
            .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
        Integer replicaCountOnBroker = JsonPath.<Integer>read(responseMessage, "KafkaBrokerState.ReplicaCountByBrokerId."
          + BROKER_ID_TO_ADD);
        return replicaCountOnBroker > 0;
    }, 200, new AssertionError("No replica found on the new broker"));
  }

}

