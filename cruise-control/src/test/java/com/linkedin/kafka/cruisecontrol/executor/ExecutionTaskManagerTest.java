/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State.*;
import static org.junit.Assert.*;


public class ExecutionTaskManagerTest {
  
  @Test
  public void testStateChangeSequences() {
    TopicPartition tp = new TopicPartition("topic", 0);
    ExecutionTaskManager taskManager = new ExecutionTaskManager(1, 1, new MetricRegistry());
    
    List<List<ExecutionTask.State>> testSequences = new ArrayList<>();
    // Completed successfully.
    testSequences.add(Arrays.asList(IN_PROGRESS, COMPLETED));
    // Rollback succeeded.
    testSequences.add(Arrays.asList(IN_PROGRESS, ABORTING, ABORTED));
    // Rollback failed.
    testSequences.add(Arrays.asList(IN_PROGRESS, ABORTING, DEAD));
    // Cannot rollback.
    testSequences.add(Arrays.asList(IN_PROGRESS, DEAD));
    
    for (List<ExecutionTask.State> sequence : testSequences) {
      taskManager.clear();
      BalancingProposal proposal = new BalancingProposal(tp, 0, 1, BalancingAction.REPLICA_MOVEMENT);
      taskManager.addBalancingProposals(Collections.singletonList(proposal), Collections.emptySet());
      List<ExecutionTask> tasks = taskManager.getReplicaMovementTasks();
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
    switch (state) {
      case IN_PROGRESS:
        taskManager.markTasksInProgress(Collections.singletonList(task));
        assertEquals(0, taskManager.remainingPartitionMovements().size());
        assertEquals(0, taskManager.remainingLeaderMovements().size());
        assertEquals(1, taskManager.inProgressTasks().size());
        assertEquals(0, taskManager.abortingTasks().size());
        assertEquals(0, taskManager.abortedTasks().size());
        assertEquals(0, taskManager.deadTasks().size());
        break;
      case ABORTING:
        taskManager.markTaskAborting(task);
        assertEquals(0, taskManager.remainingPartitionMovements().size());
        assertEquals(0, taskManager.remainingLeaderMovements().size());
        assertEquals(0, taskManager.inProgressTasks().size());
        assertEquals(1, taskManager.abortingTasks().size());
        assertEquals(0, taskManager.abortedTasks().size());
        assertEquals(0, taskManager.deadTasks().size());
        break;
      case DEAD:
        taskManager.markTaskDead(task);
        assertEquals(0, taskManager.remainingPartitionMovements().size());
        assertEquals(0, taskManager.remainingLeaderMovements().size());
        assertEquals(0, taskManager.inProgressTasks().size());
        assertEquals(0, taskManager.abortingTasks().size());
        assertEquals(0, taskManager.abortedTasks().size());
        assertEquals(1, taskManager.deadTasks().size());
        break;
      case ABORTED:
      case COMPLETED:
        ExecutionTask.State origState = task.state();
        taskManager.markTaskDone(task);
        assertEquals(0, taskManager.remainingPartitionMovements().size());
        assertEquals(0, taskManager.remainingLeaderMovements().size());
        assertEquals(0, taskManager.inProgressTasks().size());
        assertEquals(0, taskManager.abortingTasks().size());
        assertEquals(origState == ExecutionTask.State.ABORTING ? 1 : 0, taskManager.abortedTasks().size());
        assertEquals(0, taskManager.deadTasks().size());
        break;
      default:
        throw new IllegalArgumentException("Invalid state " + state);
    }
    assertEquals(state, task.state());
  }
}
