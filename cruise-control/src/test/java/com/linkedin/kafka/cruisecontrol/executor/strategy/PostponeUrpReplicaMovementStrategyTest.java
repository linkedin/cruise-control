/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;

/*
  * Unit test for testing the strategy of postponing the movement of URP replicas.
 */
public class PostponeUrpReplicaMovementStrategyTest {

  private static final Logger LOG = LoggerFactory.getLogger(PostponeUrpReplicaMovementStrategyTest.class);

  private static final TopicPartition INSYNCP0 = new TopicPartition(TOPIC0, 0);
  private static final TopicPartition INSYNCP1 = new TopicPartition(TOPIC0, 1);

  private static final TopicPartition URP0 = new TopicPartition(TOPIC1, 0);
  private static final TopicPartition URP1 = new TopicPartition(TOPIC1, 1);

  private static final TopicPartition DELETEDP0 = new TopicPartition(TOPIC2, 0);
  private static final TopicPartition DELETEDP1 = new TopicPartition(TOPIC2, 1);

  private static final int NUM_RACKS = 4;

  private static final int BROKER_ID_PLACEHOLDER = 0;

  static final long PRODUCE_SIZE_IN_BYTES = 10000L;

  static final long EXECUTION_ID_PLACEHOLDER = 0;

  static final long EXECUTION_ALERTING_THRESHOLD_MS = 100L;

  private Cluster _cluster;

  /**
   * Setup cluster for the test.
   */
  @Before
  public void setUp() {
    /*
     * Create a cluster with the following strategy:
     * 1. TOPIC0 has 2 partitions with 3 replicas each. Both the partitions have all replicas in sync
     * 2. TOPIC1 Topic has 2 partitions with 3 replicas each. Both the partitions are URPs.
     * 3. TOPIC2 has 2 partitions with 3 replicas each. This topic is deleted from the cluster.
     */
    Set<PartitionInfo> partitions = new HashSet<>();
    Node[] nodes = new Node [NUM_RACKS + 1];
    for (int i = 0; i < NUM_RACKS + 1; i++) {
      nodes[i] = new Node(i, "h" + i, 100);
    }

    // Add the insync topic partitions to the cluster
    partitions.add(new PartitionInfo(INSYNCP0.topic(), INSYNCP0.partition(), nodes[0], new Node[]{nodes[0], nodes[4], nodes[3]},
        new Node[]{nodes[0], nodes[4], nodes[3]}));
    partitions.add(new PartitionInfo(INSYNCP1.topic(), INSYNCP1.partition(), nodes[1], new Node[]{nodes[1], nodes[2], nodes[4]},
        new Node[]{nodes[1], nodes[2], nodes[4]}));

    // Add under replicated topic partitions to the cluster
    partitions.add(new PartitionInfo(URP0.topic(), URP0.partition(), nodes[1], new Node[]{nodes[1], nodes[3], nodes[2]},
        new Node[]{nodes[1], nodes[3]}));
    partitions.add(new PartitionInfo(URP1.topic(), URP1.partition(), nodes[3], new Node[]{nodes[3], nodes[4], nodes[0]},
        new Node[]{nodes[3], nodes[4]}));

    // Build the cluster object to be used by the test cases
    _cluster = new Cluster(null, Arrays.asList(nodes),
        partitions,
        Collections.emptySet(),
        Collections.emptySet());
  }

  /*
  * Testcase class for testing the task comparator.
  * Contains the following fields:
  * 1. Description of the test case
  * 2. The first task to be compared
  * 3. The second task to be compared
  * 4. The expected result of the task priority comparison
  */
  private static class TaskComparatorTestCase {
    private final String _description;
    private final ExecutionTask _task0;
    private final ExecutionTask _task1;
    private final int _expectedResult;

    TaskComparatorTestCase(String description, ExecutionTask task0, ExecutionTask task1, int expectedResult) {
      _description = description;
      _task0 = task0;
      _task1 = task1;
      _expectedResult = expectedResult;
    }

    public String description() {
      return _description;
    }

    public ExecutionTask task0() {
      return _task0;
    }

    public ExecutionTask task1() {
      return _task1;
    }

    public int expectedResult() {
      return _expectedResult;
    }

  }

  @Test
  public void testPostponseUrpReplicaMovementStrategy() {

    ReplicaPlacementInfo replicaPlacementInfoPlaceHolder = new ReplicaPlacementInfo(BROKER_ID_PLACEHOLDER);

    // Create proposals for each of the above partitions to be used for different test cases
    ExecutionProposal inSyncPartitionProposal0 =
        new ExecutionProposal(INSYNCP0, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
            Collections.singletonList(replicaPlacementInfoPlaceHolder),
            Collections.singletonList(replicaPlacementInfoPlaceHolder));
    ExecutionProposal inSyncPartitionProposal1 =
        new ExecutionProposal(INSYNCP1, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
            Collections.singletonList(replicaPlacementInfoPlaceHolder),
            Collections.singletonList(replicaPlacementInfoPlaceHolder));
    ExecutionProposal urpProposal0 =
        new ExecutionProposal(URP0, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
            Collections.singletonList(replicaPlacementInfoPlaceHolder),
            Collections.singletonList(replicaPlacementInfoPlaceHolder));
    ExecutionProposal urpProposal1 =
        new ExecutionProposal(URP1, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
            Collections.singletonList(replicaPlacementInfoPlaceHolder),
            Collections.singletonList(replicaPlacementInfoPlaceHolder));
    ExecutionProposal deletedPartitionProposal0 =
        new ExecutionProposal(DELETEDP0, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
            Collections.singletonList(replicaPlacementInfoPlaceHolder),
            Collections.singletonList(replicaPlacementInfoPlaceHolder));
    ExecutionProposal deletedPartitionProposal1 =
        new ExecutionProposal(DELETEDP1, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
            Collections.singletonList(replicaPlacementInfoPlaceHolder),
            Collections.singletonList(replicaPlacementInfoPlaceHolder));

    // Create execution tasks for each of the above proposals to be used for different test cases
    ExecutionTask inSyncPartitionTask0 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, inSyncPartitionProposal0,
        ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    ExecutionTask inSyncPartitionTask1 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, inSyncPartitionProposal1,
        ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    ExecutionTask urpTask0 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, urpProposal0,
        ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    ExecutionTask urpTask1 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, urpProposal1,
        ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    ExecutionTask deletedPartitionTask0 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, deletedPartitionProposal0,
        ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    ExecutionTask deletedPartitionTask1 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, deletedPartitionProposal1,
        ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);

    Comparator<ExecutionTask> taskComparator = new PostponeUrpReplicaMovementStrategy().taskComparator(_cluster);

    int prioritizeNone = PostponeUrpReplicaMovementStrategy.PRIORITIZE_NONE;
    int prioritizeTask1 = PostponeUrpReplicaMovementStrategy.PRIORITIZE_TASK_1;
    int prioritizeTask2 = PostponeUrpReplicaMovementStrategy.PRIORITIZE_TASK_2;

    // Test cases
    List<TaskComparatorTestCase> tcList = new ArrayList<>();

    // Test 1: both tasks are in-sync replicas - prioritize none.
    tcList.add(new TaskComparatorTestCase("Both tasks are for in-sync replica movements", inSyncPartitionTask0,
        inSyncPartitionTask1, prioritizeNone));

    // Test 2: insync task and urp task - prioritize insync task.
    tcList.add(new TaskComparatorTestCase("Task1-insync Task2-urp", inSyncPartitionTask1, urpTask1, prioritizeTask1));
    tcList.add(new TaskComparatorTestCase("Task1-urp Task2-insync", urpTask1, inSyncPartitionTask1, prioritizeTask2));

    // Test 3: insync task and deleted partition task - prioritize insync task.
    tcList.add(new TaskComparatorTestCase("Task1-insync Task2-deleted partition", inSyncPartitionTask1,
        deletedPartitionTask0, prioritizeTask1));
    tcList.add(new TaskComparatorTestCase("Task1-deleted partition Task2-insync", deletedPartitionTask0,
        inSyncPartitionTask1, prioritizeTask2));

    // Test 4: Both urp tasks - prioritize none
    tcList.add(new TaskComparatorTestCase("Both tasks are urps.", urpTask0, urpTask1, prioritizeNone));

    // Test 5: urp task and deleted partition task - prioritize None
    tcList.add(new TaskComparatorTestCase("Task1-URP Task2-Deleted Partition", urpTask1, deletedPartitionTask1, prioritizeNone));
    tcList.add(new TaskComparatorTestCase("Task1-Deleted Partition Task2-URP ", deletedPartitionTask1, urpTask1, prioritizeNone));

    // Test 6: Both deleted partition tasks - prioritize none
    tcList.add(new TaskComparatorTestCase("Both tasks are for deleted topics", deletedPartitionTask1, deletedPartitionTask0, prioritizeNone));

    // Iterate over all the test cases and run the comparator
    // Assert with the expected result
    for (TaskComparatorTestCase tc : tcList) {
      LOG.info("Test case: {}", tc.description());
      int result = taskComparator.compare(tc.task0(), tc.task1());
      Assert.assertEquals(tc.description(), tc.expectedResult(), result);
    }
  }
}
