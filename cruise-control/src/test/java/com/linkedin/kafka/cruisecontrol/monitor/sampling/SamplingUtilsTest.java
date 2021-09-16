/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.DEFAULT_CLEANUP_POLICY;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeIncreasePartitionCount;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeUpdateTopicConfig;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.wrapTopic;
import static kafka.log.LogConfig.CleanupPolicyProp;
import static kafka.log.LogConfig.RetentionMsProp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class SamplingUtilsTest {
  private static final String MOCK_TOPIC = "mock-topic";
  private static final Node MOCK_NODE = new Node(0, "mock-host", 0);
  private static final List<TopicPartitionInfo> MOCK_PARTITIONS = Collections.singletonList(
      new TopicPartitionInfo(0, MOCK_NODE, Collections.singletonList(MOCK_NODE), Collections.singletonList(MOCK_NODE)));
  private static final int MOCK_PARTITION_COUNT = 1;
  private static final int MOCK_DESIRED_PARTITION_COUNT = 2;
  private static final short MOCK_REPLICATION_FACTOR = 3;
  private static final long MOCK_DESIRED_RETENTION_MS = TimeUnit.SECONDS.toMillis(10);
  private static final String MOCK_CURRENT_RETENTION_MS = "100";
  private static final ConfigResource MOCK_TOPIC_RESOURCE = new ConfigResource(ConfigResource.Type.TOPIC, MOCK_TOPIC);

  @Test
  public void testMaybeUpdateTopicConfig() throws InterruptedException, ExecutionException, TimeoutException {
    AdminClient adminClient = EasyMock.createMock(AdminClient.class);
    DescribeConfigsResult describeConfigsResult = EasyMock.createMock(DescribeConfigsResult.class);
    KafkaFuture<Config> describedConfigsFuture = EasyMock.createMock(KafkaFuture.class);
    Config topicConfig = EasyMock.createMock(Config.class);
    AlterConfigsResult alterConfigsResult = EasyMock.createMock(AlterConfigsResult.class);
    Set<AlterConfigOp> alterConfigOps = Collections.singleton(new AlterConfigOp(
        new ConfigEntry(RetentionMsProp(), Long.toString(MOCK_DESIRED_RETENTION_MS)), AlterConfigOp.OpType.SET));
    Map<ConfigResource, KafkaFuture<Config>> describeConfigsValues = Collections.singletonMap(MOCK_TOPIC_RESOURCE,
                                                                                              describedConfigsFuture);
    Map<ConfigResource, KafkaFuture<Void>> alterConfigsValues = Collections.singletonMap(MOCK_TOPIC_RESOURCE,
                                                                                         EasyMock.createMock(KafkaFuture.class));

    NewTopic topicToUpdateConfigs = wrapTopic(MOCK_TOPIC, MOCK_PARTITION_COUNT, MOCK_REPLICATION_FACTOR, MOCK_DESIRED_RETENTION_MS);
    EasyMock.expect(adminClient.describeConfigs(EasyMock.eq(Collections.singleton(MOCK_TOPIC_RESOURCE)))).andReturn(describeConfigsResult);
    EasyMock.expect(describeConfigsResult.values()).andReturn(describeConfigsValues);
    EasyMock.expect(describedConfigsFuture.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)).andReturn(topicConfig);
    EasyMock.expect(topicConfig.get(EasyMock.eq(CleanupPolicyProp()))).andReturn(new ConfigEntry(CleanupPolicyProp(),
                                                                                                 DEFAULT_CLEANUP_POLICY));
    EasyMock.expect(topicConfig.get(EasyMock.eq(RetentionMsProp()))).andReturn(new ConfigEntry(RetentionMsProp(),
                                                                                               MOCK_CURRENT_RETENTION_MS));
    EasyMock.expect(adminClient.incrementalAlterConfigs(EasyMock.eq(Collections.singletonMap(MOCK_TOPIC_RESOURCE,
                                                                                             alterConfigOps))))
            .andReturn(alterConfigsResult);
    EasyMock.expect(alterConfigsResult.values()).andReturn(alterConfigsValues);
    EasyMock.replay(adminClient, describeConfigsResult, describedConfigsFuture, topicConfig, alterConfigsResult);

    boolean updateTopicConfig = maybeUpdateTopicConfig(adminClient, topicToUpdateConfigs);
    EasyMock.verify(adminClient, describeConfigsResult, describedConfigsFuture, topicConfig, alterConfigsResult);
    assertTrue(updateTopicConfig);
  }

  @Test
  public void testMaybeIncreasePartitionCount() throws InterruptedException, ExecutionException, TimeoutException {
    AdminClient adminClient = EasyMock.createMock(AdminClient.class);
    NewTopic topicToAddPartitions = wrapTopic(MOCK_TOPIC, MOCK_DESIRED_PARTITION_COUNT, MOCK_REPLICATION_FACTOR, MOCK_DESIRED_RETENTION_MS);
    DescribeTopicsResult describeTopicsResult = EasyMock.createMock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> topicDescriptionFuture = EasyMock.createMock(KafkaFuture.class);
    TopicDescription topicDescription = EasyMock.createMock(TopicDescription.class);
    Map<String, KafkaFuture<TopicDescription>> describeTopicsValues = Collections.singletonMap(MOCK_TOPIC, topicDescriptionFuture);
    Map<String, KafkaFuture<Void>> createPartitionsValues = Collections.singletonMap(MOCK_TOPIC, EasyMock.createMock(KafkaFuture.class));
    CreatePartitionsResult createPartitionsResult = EasyMock.createMock(CreatePartitionsResult.class);

    EasyMock.expect(adminClient.describeTopics(Collections.singletonList(MOCK_TOPIC))).andReturn(describeTopicsResult);
    EasyMock.expect(describeTopicsResult.values()).andReturn(describeTopicsValues);
    EasyMock.expect(topicDescriptionFuture.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)).andReturn(topicDescription);
    EasyMock.expect(topicDescription.partitions()).andReturn(MOCK_PARTITIONS);
    EasyMock.expect(adminClient.createPartitions(Collections.singletonMap(MOCK_TOPIC, EasyMock.anyObject())))
            .andReturn(createPartitionsResult);
    EasyMock.expect(createPartitionsResult.values()).andReturn(createPartitionsValues);

    EasyMock.replay(adminClient, describeTopicsResult, topicDescriptionFuture, topicDescription, createPartitionsResult);
    KafkaCruiseControlUtils.CompletionType increasePartitionCount = maybeIncreasePartitionCount(adminClient, topicToAddPartitions);

    EasyMock.verify(adminClient, describeTopicsResult, topicDescriptionFuture, topicDescription, createPartitionsResult);
    assertEquals(KafkaCruiseControlUtils.CompletionType.COMPLETED, increasePartitionCount);
  }
}
