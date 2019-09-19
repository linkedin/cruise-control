/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State.*;
import static org.junit.Assert.assertEquals;


public class ExecutionTaskManagerTest {

  private Cluster generateExpectedCluster(ExecutionProposal proposal, TopicPartition tp) {
    List<Node> expectedReplicas = new ArrayList<>(proposal.oldReplicas().size());
    expectedReplicas.add(new Node(0, "null", -1));
    expectedReplicas.add(new Node(2, "null", -1));

    Node[] isrArray = new Node[expectedReplicas.size()];
    isrArray = expectedReplicas.toArray(isrArray);

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(new PartitionInfo(tp.topic(), tp.partition(), expectedReplicas.get(1), isrArray, isrArray));

    return new Cluster(null, expectedReplicas, partitions, Collections.emptySet(), Collections.emptySet());
  }

  @Test
  public void testStateChangeSequences() {
    TopicPartition tp = new TopicPartition("topic", 0);
    ExecutionTaskManager taskManager = new ExecutionTaskManager(null, new MetricRegistry(), new SystemTime(),
            new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    List<List<ExecutionTask.State>> testSequences = new ArrayList<>();
    // Completed successfully.
    testSequences.add(Arrays.asList(IN_PROGRESS, COMPLETED));
    // Rollback succeeded.
    testSequences.add(Arrays.asList(IN_PROGRESS, ABORTING, ABORTED));
    // Rollback failed.
    testSequences.add(Arrays.asList(IN_PROGRESS, ABORTING, DEAD));
    // Cannot rollback.
    testSequences.add(Arrays.asList(IN_PROGRESS, DEAD));

    ReplicaPlacementInfo r0 = new ReplicaPlacementInfo(0);
    ReplicaPlacementInfo r1 = new ReplicaPlacementInfo(1);
    ReplicaPlacementInfo r2 = new ReplicaPlacementInfo(2);
    for (List<ExecutionTask.State> sequence : testSequences) {
      taskManager.clear();
      // Make sure the proposal does not involve leader movement.
      ExecutionProposal proposal =
          new ExecutionProposal(tp, 10, r2, Arrays.asList(r0, r2), Arrays.asList(r2, r1));

      taskManager.setExecutionModeForTaskTracker(false);
      taskManager.addExecutionProposals(Collections.singletonList(proposal),
                                        Collections.emptySet(),
                                        generateExpectedCluster(proposal, tp),
                                        null);
      taskManager.setRequestedInterBrokerPartitionMovementConcurrency(null);
      taskManager.setRequestedIntraBrokerPartitionMovementConcurrency(null);
      taskManager.setRequestedLeadershipMovementConcurrency(null);
      List<ExecutionTask> tasks = taskManager.getInterBrokerReplicaMovementTasks();
      assertEquals(1, tasks.size());
      ExecutionTask task  = tasks.get(0);
      verifyStateChangeSequence(sequence, task, taskManager);
    }
  }

  private void verifyStateChangeSequence(List<ExecutionTask.State> stateSequence,
                                         ExecutionTask task,
                                         ExecutionTaskManager taskManager) {
    stateSequence.forEach(s -> changeTaskState(s, task, taskManager));
  }

  private void changeTaskState(ExecutionTask.State state, ExecutionTask task, ExecutionTaskManager taskManager) {
    ExecutionTaskTracker.ExecutionTasksSummary executionTasksSummary;
    Map<ExecutionTask.State, Integer> taskStat;
    switch (state) {
      case IN_PROGRESS:
        taskManager.markTasksInProgress(Collections.singletonList(task));
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(PENDING));
        assertEquals(1, (int) taskStat.get(IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ABORTING));
        assertEquals(0, (int) taskStat.get(ABORTED));
        assertEquals(0, (int) taskStat.get(COMPLETED));
        assertEquals(0, (int) taskStat.get(DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(10, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(0, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case ABORTING:
        taskManager.markTaskAborting(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(PENDING));
        assertEquals(0, (int) taskStat.get(IN_PROGRESS));
        assertEquals(1, (int) taskStat.get(ABORTING));
        assertEquals(0, (int) taskStat.get(ABORTED));
        assertEquals(0, (int) taskStat.get(COMPLETED));
        assertEquals(0, (int) taskStat.get(DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(10, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(0, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case DEAD:
        taskManager.markTaskDead(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(PENDING));
        assertEquals(0, (int) taskStat.get(IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ABORTING));
        assertEquals(0, (int) taskStat.get(ABORTED));
        assertEquals(0, (int) taskStat.get(COMPLETED));
        assertEquals(1, (int) taskStat.get(DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(0, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(10, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case ABORTED:
      case COMPLETED:
        ExecutionTask.State origState = task.state();
        taskManager.markTaskDone(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(PENDING));
        assertEquals(0, (int) taskStat.get(IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ABORTING));
        assertEquals(origState == ExecutionTask.State.ABORTING ? 1 : 0, (int) taskStat.get(ABORTED));
        assertEquals(origState == ExecutionTask.State.ABORTING ? 0 : 1, (int) taskStat.get(COMPLETED));
        assertEquals(0, (int) taskStat.get(DEAD));
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
