/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringSerializer;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.*;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEventTopicReader.DEFAULT_MAINTENANCE_PLAN_EXPIRATION_MS;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEventTopicReader.MAINTENANCE_EVENT_TOPIC_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEventTopicReader.MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEventTopicReader.MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEventTopicReader.MAINTENANCE_EVENT_TOPIC_RETENTION_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEventType.REBALANCE;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.MAINTENANCE_EVENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class MaintenanceEventTopicReaderTest extends CruiseControlIntegrationTestHarness {
  public static final String TEST_TOPIC = "__CloudMaintenanceEvent";
  public static final String TEST_TOPIC_REPLICATION_FACTOR = "1";
  public static final String TEST_TOPIC_PARTITION_COUNT = "8";
  public static final String TEST_TOPIC_RETENTION_TIME_MS = Long.toString(TimeUnit.HOURS.toMillis(1));
  public static final String RETENTION_MS_CONFIG = "retention.ms";
  public static final long TEST_REBALANCE_PLAN_TIME = 1601089200000L;
  public static final long TEST_EXPIRED_PLAN_TIME = TEST_REBALANCE_PLAN_TIME - 1L;
  public static final int TEST_BROKER_ID = 42;
  public static final Duration TEST_TIMEOUT = Duration.ofSeconds(5);
  public static final Map<Short, String> TEST_TOPIC_REGEX_WITH_RF_UPDATE = Collections.singletonMap((short) 2, "T1");
  private TopicDescription _topicDescription;
  private Config _topicConfig;

  /**
   * Setup the unit test and produce maintenance plans to the maintenance topic.
   */
  @Before
  public void setup() throws Exception {
    super.start();
    produceMaintenancePlans();
  }

  /**
   * Produces plans to the {@link #TEST_TOPIC}.
   * All plans except {@link RebalancePlan} are expired.
   */
  private void produceMaintenancePlans() {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    try (Producer<String, MaintenancePlan> producer = createMaintenancePlanProducer(props)) {
      sendPlan(producer, new RebalancePlan(TEST_REBALANCE_PLAN_TIME, TEST_BROKER_ID));
      sendPlan(producer, new FixOfflineReplicasPlan(TEST_EXPIRED_PLAN_TIME, TEST_BROKER_ID));
      sendPlan(producer, new DemoteBrokerPlan(TEST_EXPIRED_PLAN_TIME, TEST_BROKER_ID, Collections.singleton(TEST_BROKER_ID)));
      sendPlan(producer, new AddBrokerPlan(TEST_EXPIRED_PLAN_TIME, TEST_BROKER_ID, Collections.singleton(TEST_BROKER_ID)));
      sendPlan(producer, new RemoveBrokerPlan(TEST_EXPIRED_PLAN_TIME, TEST_BROKER_ID, Collections.singleton(TEST_BROKER_ID)));
      sendPlan(producer, new TopicReplicationFactorPlan(TEST_EXPIRED_PLAN_TIME, TEST_BROKER_ID, TEST_TOPIC_REGEX_WITH_RF_UPDATE));
    }
  }

  @javax.annotation.Nonnull
  protected Producer<String, MaintenancePlan> createMaintenancePlanProducer(Properties overrides) {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MaintenancePlanSerde.class.getCanonicalName());
    //apply overrides
    if (overrides != null) {
      props.putAll(overrides);
    }
    return new KafkaProducer<>(props);
  }

  private void sendPlan(Producer<String, MaintenancePlan> producer, MaintenancePlan maintenancePlan) {
    producer.send(new ProducerRecord<>(TEST_TOPIC, maintenancePlan), (recordMetadata, e) -> {
      if (e != null) {
        fail("Failed to produce maintenance plan");
      }
    });
  }

  @After
  public void teardown() {
    super.stop();
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = new HashMap<>(5);
    configs.put(MAINTENANCE_EVENT_TOPIC_CONFIG, TEST_TOPIC);
    configs.put(MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR_CONFIG, TEST_TOPIC_REPLICATION_FACTOR);
    configs.put(MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT_CONFIG, TEST_TOPIC_PARTITION_COUNT);
    configs.put(MAINTENANCE_EVENT_TOPIC_RETENTION_MS_CONFIG, TEST_TOPIC_RETENTION_TIME_MS);
    configs.put(AnomalyDetectorConfig.MAINTENANCE_EVENT_READER_CLASS_CONFIG, MaintenanceEventTopicReader.class.getName());
    return configs;
  }

  /**
   * Retrieve the latest metadata for {@link #_topicDescription} and {@link #_topicConfig} topics.
   * To ensure the latest metadata update, admin clients retrieve the metadata from both brokers and ensure that they
   * have the same metadata.
   */
  private void retrieveLatestMetadata() throws InterruptedException, ExecutionException {
    TopicDescription description0;
    TopicDescription description1;
    Config topicConfig0;
    Config topicConfig1;
    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, TEST_TOPIC);
    AdminClient adminClient0 = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    AdminClient adminClient1 = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(1).plaintextAddr()));
    try {
      while (true) {
        description0 = adminClient0.describeTopics(Collections.singleton(TEST_TOPIC)).values().get(TEST_TOPIC).get();
        description1 = adminClient1.describeTopics(Collections.singleton(TEST_TOPIC)).values().get(TEST_TOPIC).get();
        topicConfig0 = adminClient0.describeConfigs(Collections.singleton(topicResource)).values().get(topicResource).get();
        topicConfig1 = adminClient1.describeConfigs(Collections.singleton(topicResource)).values().get(topicResource).get();
        if (description0 != null && description1 != null && topicConfig0 != null && topicConfig1 != null
            && description0.partitions().size() == description1.partitions().size()
            && description0.partitions().get(0).replicas().size() == description1.partitions().get(0).replicas().size()
            && topicConfig0.get(RETENTION_MS_CONFIG).value().equals(topicConfig1.get(RETENTION_MS_CONFIG).value())) {
          _topicDescription = description0;
          _topicConfig = topicConfig0;
          break;
        }
        Thread.sleep(50);
      }
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient0);
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient1);
    }
  }

  private void verify(String partitionCount, String replicationFactor, String retentionMs, boolean testingTopicConfigUpdate)
      throws ExecutionException, InterruptedException {
    retrieveLatestMetadata();
    // Verify that the maintenance event topic has been created with the desired properties.
    assertEquals(Integer.parseInt(partitionCount), _topicDescription.partitions().size());
    if (!testingTopicConfigUpdate) {
      assertEquals(Integer.parseInt(replicationFactor), _topicDescription.partitions().get(0).replicas().size());
    }

    String currentRetentionMs = _topicConfig.get(RETENTION_MS_CONFIG).value();
    assertEquals(retentionMs, currentRetentionMs);
  }

  @Test
  public void testMaintenanceEventTopicCreationUpdateAndRead()
      throws ExecutionException, InterruptedException, SamplingException {
    // Verify that the maintenance event topic has been created with the desired properties.
    verify(TEST_TOPIC_PARTITION_COUNT, TEST_TOPIC_REPLICATION_FACTOR, TEST_TOPIC_RETENTION_TIME_MS, false);

    // Verify that the maintenance event topic properties can be updated if the topic already exists with different properties.
    String newPartitionCount = String.valueOf(Integer.parseInt(TEST_TOPIC_PARTITION_COUNT) * 2);
    String newRF = String.valueOf(Short.parseShort(TEST_TOPIC_REPLICATION_FACTOR) + 1);
    String newRetentionMs = String.valueOf(Long.MAX_VALUE);

    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    Map<String, Object> parameterConfigOverrides = new HashMap<>(4);
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, mockKafkaCruiseControl);
    parameterConfigOverrides.put(MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR_CONFIG, newRF);
    parameterConfigOverrides.put(MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT_CONFIG, newPartitionCount);
    parameterConfigOverrides.put(MAINTENANCE_EVENT_TOPIC_RETENTION_MS_CONFIG, newRetentionMs);

    // The current time is expected to cause (1) a valid rebalance plan creation, but (2) an expired demote broker plan.
    long currentMockTime = TEST_REBALANCE_PLAN_TIME + DEFAULT_MAINTENANCE_PLAN_EXPIRATION_MS;
    EasyMock.expect(mockKafkaCruiseControl.timeMs()).andReturn(currentMockTime).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.adminClient())
            .andReturn(createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(_config))).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(_config).anyTimes();

    EasyMock.replay(mockKafkaCruiseControl);

    MaintenanceEventReader maintenanceEventReader = _config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_READER_CLASS_CONFIG,
                                                                                  MaintenanceEventTopicReader.class,
                                                                                  parameterConfigOverrides);

    assertNotNull(maintenanceEventReader);
    verify(newPartitionCount, newRF, newRetentionMs, true);
    Set<MaintenanceEvent> events = maintenanceEventReader.readEvents(TEST_TIMEOUT);
    EasyMock.verify(mockKafkaCruiseControl);
    assertEquals(1, events.size());
    MaintenanceEvent maintenanceEvent = events.iterator().next();
    assertEquals(MAINTENANCE_EVENT, maintenanceEvent.anomalyType());
    assertEquals(currentMockTime, maintenanceEvent.detectionTimeMs());
    assertEquals(REBALANCE, maintenanceEvent.maintenanceEventType());
  }
}
