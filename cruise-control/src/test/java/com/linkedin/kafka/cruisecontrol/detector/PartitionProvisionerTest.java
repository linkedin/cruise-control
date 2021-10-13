/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CompletionType.COMPLETED;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.sync.RightsizeRequest.RECOMMENDER_UP;
import static org.junit.Assert.assertEquals;


/**
 * A partition provisioner test based on using {@link BasicProvisioner} for partition provisioning.
 */
public class PartitionProvisionerTest extends AbstractProvisionerTest {
  private static final AdminClient MOCK_ADMIN_CLIENT = EasyMock.createMock(AdminClient.class);
  // Mocks for describe topics
  private static final DescribeTopicsResult MOCK_DESCRIBE_TOPICS_RESULT = EasyMock.createMock(DescribeTopicsResult.class);
  private static final KafkaFuture<TopicDescription> MOCK_TOPIC_DESCRIPTION_FUTURE = EasyMock.createMock(KafkaFuture.class);
  // Mocks for create partitions
  private static final CreatePartitionsResult MOCK_CREATE_PARTITIONS_RESULT = EasyMock.createMock(CreatePartitionsResult.class);
  private static final KafkaFuture<Void> MOCK_CREATE_PARTITIONS_FUTURE = EasyMock.createMock(KafkaFuture.class);

  private static final String MOCK_TOPIC = "mock-topic";
  public static final List<Node> NODES = Collections.singletonList(new Node(0, "host0", 0));
  private static final Cluster MOCK_KAFKA_CLUSTER;
  static {
    Set<PartitionInfo> partitions = Collections.singleton(new PartitionInfo(MOCK_TOPIC, 0, NODES.get(0), NODES.toArray(new Node[1]),
                                                                            NODES.toArray(new Node[1])));
    MOCK_KAFKA_CLUSTER = new Cluster("id", NODES, partitions, Collections.emptySet(), Collections.emptySet(), NODES.get(0));
  }
  private static final TopicPartitionInfo MOCK_TOPIC_PARTITION_INFO = new TopicPartitionInfo(0, NODES.get(0), NODES, NODES);
  private static final TopicDescription MOCK_TOPIC_DESCRIPTION = new TopicDescription(MOCK_TOPIC, false,
                                                                                      Collections.singletonList(MOCK_TOPIC_PARTITION_INFO));
  // Create resources for (1) the cluster and (2) each broker.
  private static final Pattern MOCK_TOPIC_PATTERN = Pattern.compile(MOCK_TOPIC);
  private static final int MOCK_IGNORED_PARTITION_COUNT = 1;
  private static final int MOCK_PARTITION_COUNT = 2;

  /**
   * Execute before every test case.
   */
  @Before
  @Override
  public void setUp() {
    super.setUp();
    EasyMock.expect(MOCK_KAFKA_CRUISE_CONTROL.adminClient()).andReturn(MOCK_ADMIN_CLIENT);
    EasyMock.expect(MOCK_KAFKA_CRUISE_CONTROL.kafkaCluster()).andReturn(MOCK_KAFKA_CLUSTER);
    EasyMock.replay(MOCK_KAFKA_CRUISE_CONTROL);
  }

  /**
   * Execute after every test case.
   */
  @After
  public void teardown() {
    EasyMock.verify(MOCK_KAFKA_CRUISE_CONTROL, MOCK_ADMIN_CLIENT, MOCK_DESCRIBE_TOPICS_RESULT, MOCK_TOPIC_DESCRIPTION_FUTURE,
                    MOCK_CREATE_PARTITIONS_FUTURE, MOCK_CREATE_PARTITIONS_RESULT);
    EasyMock.reset(MOCK_KAFKA_CRUISE_CONTROL, MOCK_ADMIN_CLIENT, MOCK_DESCRIBE_TOPICS_RESULT, MOCK_TOPIC_DESCRIPTION_FUTURE,
                   MOCK_CREATE_PARTITIONS_FUTURE, MOCK_CREATE_PARTITIONS_RESULT);
  }

  @Test
  public void testProvisionPartitionIncreaseConstructsCompletedResponse()
      throws ExecutionException, InterruptedException, TimeoutException {
    ProvisionerState.State expectedState = ProvisionerState.State.COMPLETED;
    String expectedSummary = String.format("[Recommender-Under-Provisioned] Setting partition count by topic || Succeeded: {%s=%d}.",
                                           MOCK_TOPIC, MOCK_PARTITION_COUNT);
    assertProvisionPartitionIncreaseConstructsCorrectResponse(COMPLETED, expectedState, expectedSummary);
  }

  @Test
  public void testProvisionPartitionIncreaseConstructsCompletedWithIgnoreResponse()
      throws ExecutionException, InterruptedException, TimeoutException {
    ProvisionerState.State expectedState = ProvisionerState.State.COMPLETED;
    String expectedSummary = String.format("[Recommender-Under-Provisioned] Setting partition count by topic || Ignored: {%s=%d}.",
                                           MOCK_TOPIC, MOCK_IGNORED_PARTITION_COUNT);
    assertProvisionPartitionIncreaseConstructsCorrectResponse(KafkaCruiseControlUtils.CompletionType.NO_ACTION, expectedState, expectedSummary);
  }

  @Test
  public void testProvisionPartitionIncreaseConstructsCompletedWithErrorResponse()
      throws ExecutionException, InterruptedException, TimeoutException {
    ProvisionerState.State expectedState = ProvisionerState.State.COMPLETED_WITH_ERROR;
    String expectedSummary = String.format("[Recommender-Under-Provisioned] Setting partition count by topic || Failed: {%s=%d}.",
                                           MOCK_TOPIC, MOCK_PARTITION_COUNT);
    assertProvisionPartitionIncreaseConstructsCorrectResponse(KafkaCruiseControlUtils.CompletionType.COMPLETED_WITH_ERROR, expectedState,
                                                              expectedSummary);
  }

  @Test
  public void testProvisionPartitionIncreaseWithBrokerRecommendation()
      throws ExecutionException, InterruptedException, TimeoutException {
    ProvisionerState.State expectedState = ProvisionerState.State.COMPLETED;

    String expectedBrokerSummary = String.format("Provisioner support is missing. Skip recommendation: %s", BROKER_REC_TO_EXECUTE);
    String expectedSummary = String.format("[Recommender-Under-Provisioned] Setting partition count by topic || Succeeded: {%s=%d}. || %s",
                                           MOCK_TOPIC, MOCK_PARTITION_COUNT, expectedBrokerSummary);
    assertProvisionPartitionIncreaseConstructsCorrectResponse(COMPLETED, expectedState, expectedSummary, true);
  }

  private void assertProvisionPartitionIncreaseConstructsCorrectResponse(KafkaCruiseControlUtils.CompletionType partitionIncreaseCompletion,
                                                                         ProvisionerState.State expectedState,
                                                                         String expectedSummary)
      throws ExecutionException, InterruptedException, TimeoutException {
    assertProvisionPartitionIncreaseConstructsCorrectResponse(partitionIncreaseCompletion, expectedState, expectedSummary, false);
  }

  private void assertProvisionPartitionIncreaseConstructsCorrectResponse(KafkaCruiseControlUtils.CompletionType partitionIncreaseCompletion,
                                                                         ProvisionerState.State expectedState,
                                                                         String expectedSummary,
                                                                         boolean hasBrokerRecommendation)
      throws ExecutionException, InterruptedException, TimeoutException {
    int recommendedPartitionCount = expectedSummary.contains("Ignored") ? MOCK_IGNORED_PARTITION_COUNT : MOCK_PARTITION_COUNT;
    ProvisionRecommendation recommendation =
        new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numPartitions(recommendedPartitionCount)
                                                                              .topicPattern(MOCK_TOPIC_PATTERN).build();
    Map<String, ProvisionRecommendation> provisionRecommendation;
    if (hasBrokerRecommendation) {
      provisionRecommendation = Map.of(RECOMMENDER_UP, recommendation, RECOMMENDER_TO_EXECUTE, BROKER_REC_TO_EXECUTE);
    } else {
      provisionRecommendation = Collections.singletonMap(RECOMMENDER_UP, recommendation);
    }
    Map<String, KafkaFuture<TopicDescription>> describeTopicsValues = Collections.singletonMap(MOCK_TOPIC, MOCK_TOPIC_DESCRIPTION_FUTURE);

    EasyMock.expect(MOCK_ADMIN_CLIENT.describeTopics(Collections.singletonList(MOCK_TOPIC))).andReturn(MOCK_DESCRIBE_TOPICS_RESULT);
    EasyMock.expect(MOCK_DESCRIBE_TOPICS_RESULT.values()).andReturn(describeTopicsValues);

    if (partitionIncreaseCompletion == COMPLETED) {
      EasyMock.expect(MOCK_TOPIC_DESCRIPTION_FUTURE.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)).andReturn(MOCK_TOPIC_DESCRIPTION);

      // Create partitions: for this test, we ignore the fact that the mock cluster has one node -- i.e. in reality a request to increase
      // partition count to two would fail in a cluster with one node.
      EasyMock.expect(MOCK_ADMIN_CLIENT.createPartitions(Collections.singletonMap(MOCK_TOPIC, EasyMock.anyObject())))
              .andReturn(MOCK_CREATE_PARTITIONS_RESULT);
      Map<String, KafkaFuture<Void>> createPartitionsResultValues = Collections.singletonMap(MOCK_TOPIC, MOCK_CREATE_PARTITIONS_FUTURE);
      EasyMock.expect(MOCK_CREATE_PARTITIONS_RESULT.values()).andReturn(createPartitionsResultValues);
      EasyMock.expect(MOCK_CREATE_PARTITIONS_FUTURE.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)).andReturn(null);

    } else if (partitionIncreaseCompletion == KafkaCruiseControlUtils.CompletionType.COMPLETED_WITH_ERROR) {
      EasyMock.expect(MOCK_TOPIC_DESCRIPTION_FUTURE.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS))
              .andThrow(new ExecutionException(new InvalidTopicException()));
    } else {
      EasyMock.expect(MOCK_TOPIC_DESCRIPTION_FUTURE.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)).andReturn(MOCK_TOPIC_DESCRIPTION);
    }

    EasyMock.replay(MOCK_ADMIN_CLIENT, MOCK_DESCRIBE_TOPICS_RESULT, MOCK_TOPIC_DESCRIPTION_FUTURE,
                    MOCK_CREATE_PARTITIONS_FUTURE, MOCK_CREATE_PARTITIONS_RESULT);

    ProvisionerState results = _provisioner.rightsize(provisionRecommendation, RIGHTSIZE_OPTIONS);

    assertEquals(expectedState, results.state());
    assertEquals(expectedSummary, results.summary());
  }
}
