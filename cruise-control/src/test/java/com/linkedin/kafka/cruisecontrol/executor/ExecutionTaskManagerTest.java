/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.StrategyOptions;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class ExecutionTaskManagerTest {
  private static final Map<ConcurrencyType, Integer> MOCK_DEFAULT_CONCURRENCY;
  static {
    MOCK_DEFAULT_CONCURRENCY =
        Map.of(ConcurrencyType.INTER_BROKER_REPLICA, 4, ConcurrencyType.LEADERSHIP, 500, ConcurrencyType.INTRA_BROKER_REPLICA, 2);
  }
  private static ExecutionTaskManager taskManager;

  private Cluster generateExpectedCluster(ExecutionProposal proposal) {
    List<Node> expectedReplicas = new ArrayList<>(proposal.oldReplicas().size());
    expectedReplicas.add(new Node(0, "null", -1));
    expectedReplicas.add(new Node(2, "null", -1));

    Node[] isrArray = new Node[expectedReplicas.size()];
    isrArray = expectedReplicas.toArray(isrArray);

    TopicPartition tp = proposal.topicPartition();
    Set<PartitionInfo> partition = Collections.singleton(new PartitionInfo(tp.topic(), tp.partition(), expectedReplicas.get(1), isrArray, isrArray));

    return new Cluster(null, expectedReplicas, partition, Collections.emptySet(), Collections.emptySet());
  }

  /**
   * Setup the test.
   */
  @BeforeClass
  public static void setup() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.put(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                   Integer.toString(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.INTER_BROKER_REPLICA)));
    properties.put(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG,
                   Integer.toString(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.LEADERSHIP)));
    properties.put(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG,
                   Integer.toString(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.INTRA_BROKER_REPLICA)));
    taskManager = new ExecutionTaskManager(null, new MetricRegistry(), new SystemTime(),
                                           new KafkaCruiseControlConfig(properties));
  }

  @Test
  public void testStateChangeSequences() {
    TopicPartition tp = new TopicPartition("topic", 0);

    List<List<ExecutionTaskState>> testSequences = new ArrayList<>();
    // Completed successfully.
    testSequences.add(Arrays.asList(ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.COMPLETED));
    // Rollback succeeded.
    testSequences.add(Arrays.asList(ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.ABORTING, ExecutionTaskState.ABORTED));
    // Rollback failed.
    testSequences.add(Arrays.asList(ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.ABORTING, ExecutionTaskState.DEAD));
    // Cannot rollback.
    testSequences.add(Arrays.asList(ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.DEAD));

    ReplicaPlacementInfo r0 = new ReplicaPlacementInfo(0);
    ReplicaPlacementInfo r1 = new ReplicaPlacementInfo(1);
    ReplicaPlacementInfo r2 = new ReplicaPlacementInfo(2);
    // Make sure the proposal does not involve leader movement.
    ExecutionProposal proposal = new ExecutionProposal(tp, 10, r2, Arrays.asList(r0, r2), Arrays.asList(r2, r1));
    StrategyOptions strategyOptions = new StrategyOptions.Builder(generateExpectedCluster(proposal)).build();

    for (List<ExecutionTaskState> sequence : testSequences) {
      taskManager.clear();
      taskManager.setExecutionModeForTaskTracker(false);
      taskManager.addExecutionProposals(Collections.singletonList(proposal),
                                        Collections.emptySet(),
                                        strategyOptions,
                                        null);
      taskManager.setRequestedInterBrokerPartitionMovementConcurrency(null);
      taskManager.setRequestedIntraBrokerPartitionMovementConcurrency(null);
      taskManager.setRequestedLeadershipMovementConcurrency(null);
      List<ExecutionTask> tasks = taskManager.getInterBrokerReplicaMovementTasks();
      assertEquals(1, tasks.size());
      ExecutionTask task = tasks.get(0);
      verifyStateChangeSequence(sequence, task, taskManager);
    }

    // Verify that the movement concurrency matches the default configuration
    for (ConcurrencyType concurrencyType : ConcurrencyType.cachedValues()) {
      assertEquals(MOCK_DEFAULT_CONCURRENCY.get(concurrencyType).intValue(), taskManager.movementConcurrency(concurrencyType));
    }
  }

  private void verifyStateChangeSequence(List<ExecutionTaskState> stateSequence,
                                         ExecutionTask task,
                                         ExecutionTaskManager taskManager) {
    stateSequence.forEach(s -> changeTaskState(s, task, taskManager));
  }

  private void changeTaskState(ExecutionTaskState state, ExecutionTask task, ExecutionTaskManager taskManager) {
    ExecutionTaskTracker.ExecutionTasksSummary executionTasksSummary;
    Map<ExecutionTaskState, Integer> taskStat;
    switch (state) {
      case IN_PROGRESS:
        taskManager.markTasksInProgress(Collections.singletonList(task));
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.PENDING));
        assertEquals(1, (int) taskStat.get(ExecutionTaskState.IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.COMPLETED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(10, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(0, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case ABORTING:
        taskManager.markTaskAborting(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.PENDING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.IN_PROGRESS));
        assertEquals(1, (int) taskStat.get(ExecutionTaskState.ABORTING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.COMPLETED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(10, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(0, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case DEAD:
        taskManager.markTaskDead(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.PENDING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.COMPLETED));
        assertEquals(1, (int) taskStat.get(ExecutionTaskState.DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(0, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(10, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case ABORTED:
      case COMPLETED:
        ExecutionTaskState origState = task.state();
        taskManager.markTaskDone(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.PENDING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTING));
        assertEquals(origState == ExecutionTaskState.ABORTING ? 1 : 0, (int) taskStat.get(ExecutionTaskState.ABORTED));
        assertEquals(origState == ExecutionTaskState.ABORTING ? 0 : 1, (int) taskStat.get(ExecutionTaskState.COMPLETED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(0, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(10, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      default:
        throw new IllegalArgumentException("Invalid state " + state);
    }
    assertEquals(state, task.state());
  }
}
