/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExecutionUtilsTest {

  @Test
  public void testProcessAlterPartitionReassignmentResult() throws Exception {
    // Case 1: Handle null result input. Expect no side effect.
    ExecutionUtils.processAlterPartitionReassignmentsResult(null, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

    String topicName = "topic-name";
    int partitionId = 0;

    // Case 2: Handle successful execution results
    AlterPartitionReassignmentsResult result = EasyMock.mock(AlterPartitionReassignmentsResult.class);
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(topicName, partitionId, null))
            .once();
    EasyMock.replay(result);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 3: Handle invalid replica assignment exception
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(topicName, partitionId, new ExecutionException(Errors.INVALID_REPLICA_ASSIGNMENT.exception())))
            .once();
    EasyMock.replay(result);
    Set<TopicPartition> deadTopicPartitions = new HashSet<>(1);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), deadTopicPartitions, Collections.emptySet());
    assertEquals(Collections.singleton(new TopicPartition(topicName, partitionId)), deadTopicPartitions);
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 4: Handle unknown topic or partition exception
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(topicName, partitionId, new ExecutionException(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception())))
            .once();
    EasyMock.replay(result);
    Set<TopicPartition> deletedTopicPartitions = new HashSet<>(1);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, deletedTopicPartitions, Collections.emptySet(), Collections.emptySet());
    assertEquals(Collections.singleton(new TopicPartition(topicName, partitionId)), deletedTopicPartitions);
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 5: Handle no reassign in progress exception
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(topicName, partitionId, new ExecutionException(Errors.NO_REASSIGNMENT_IN_PROGRESS.exception())))
            .once();
    EasyMock.replay(result);
    Set<TopicPartition> noReassignmentToCancelTopicPartitions = new HashSet<>(1);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result,
                                                            Collections.emptySet(),
                                                            Collections.emptySet(),
                                                            noReassignmentToCancelTopicPartitions);
    assertEquals(Collections.singleton(new TopicPartition(topicName, partitionId)), noReassignmentToCancelTopicPartitions);
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 6: Handle execution timeout exception. Expect no side effect.
    org.apache.kafka.common.errors.TimeoutException kafkaTimeoutException = new org.apache.kafka.common.errors.TimeoutException();
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(topicName, partitionId, new ExecutionException(kafkaTimeoutException)))
            .once();
    EasyMock.replay(result);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 7: Handle future wait timeout exception. Expect no side effect.
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(topicName, partitionId, new TimeoutException()))
            .once();
    EasyMock.replay(result);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 8: Handle future wait interrupted exception
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(topicName, partitionId, new InterruptedException()))
            .once();
    EasyMock.replay(result);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    EasyMock.verify(result);
    EasyMock.reset(result);
  }

  private Map<TopicPartition, KafkaFuture<Void>> getKafkaFutureByTopicPartition(String topicName,
                                                                                int partitionId,
                                                                                @Nullable Exception futureException) throws Exception {
    Map<TopicPartition, KafkaFuture<Void>> futureByTopicPartition = new HashMap<>(1);
    KafkaFuture<Void> kafkaFuture = EasyMock.mock(KafkaFuture.class);
    if (futureException == null) {
      EasyMock.expect(kafkaFuture.get(ExecutionUtils.EXECUTION_TASK_FUTURE_ERROR_VERIFICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
              .andReturn(null).once();
    } else {
      EasyMock.expect(kafkaFuture.get(ExecutionUtils.EXECUTION_TASK_FUTURE_ERROR_VERIFICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
              .andThrow(futureException).once();
    }
    EasyMock.replay(kafkaFuture);
    futureByTopicPartition.put(new TopicPartition(topicName, partitionId), kafkaFuture);
    return futureByTopicPartition;
  }
}
