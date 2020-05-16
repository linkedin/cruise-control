/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionUtils.class);
  // TODO: Make this configurable.
  public static final long DEFAULT_LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS = 10000L;

  private ExecutionUtils() { }

  /**
   * Retrieve the set of {@link TopicPartition partitions} that are currently being reassigned.
   *
   * @param adminClient The adminClient to ask for ongoing partition reassignments.
   * @return The set of {@link TopicPartition partitions} that are being reassigned.
   */
  public static Set<TopicPartition> partitionsBeingReassigned(AdminClient adminClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    return ongoingPartitionReassignments(adminClient).keySet();
  }

  /**
   * Retrieve the map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
   *
   * @param adminClient The adminClient to ask for ongoing partition reassignments.
   * @return The map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
   */
  public static Map<TopicPartition, PartitionReassignment> ongoingPartitionReassignments(AdminClient adminClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    ListPartitionReassignmentsResult responseResult = adminClient.listPartitionReassignments();
    return responseResult.reassignments().get(DEFAULT_LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private static Optional<NewPartitionReassignment> cancelReassignmentValue() {
    return java.util.Optional.empty();
  }

  private static Optional<NewPartitionReassignment> reassignmentValue(List<Integer> targetReplicas) {
    return Optional.of(new NewPartitionReassignment(targetReplicas));
  }

  /**
   * Submits the given inter-broker replica reassignment tasks for execution using the given admin client.
   *
   * @param adminClient The adminClient to retrieve ongoing replica reassignments and submit new reassignments.
   * @param tasks Inter-broker replica reassignment tasks to execute.
   * @return The {@link AlterPartitionReassignmentsResult result} of reassignment request, {@code null} otherwise.
   */
  public static AlterPartitionReassignmentsResult submitReplicaReassignmentTasks(AdminClient adminClient, List<ExecutionTask> tasks)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (tasks == null || tasks.isEmpty()) {
      throw new IllegalArgumentException(String.format("Tasks to execute (%s) cannot be null or empty.", tasks));
    }

    Map<TopicPartition, PartitionReassignment> ongoingReassignments = ongoingPartitionReassignments(adminClient);
    // Update the ongoing replica reassignments in case the task status has changed.
    Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments = new HashMap<>(tasks.size());
    for (ExecutionTask task : tasks) {
      TopicPartition tp = task.proposal().topicPartition();
      List<Integer> newReplicas = new ArrayList<>(task.proposal().newReplicas().size());
      for (ReplicaPlacementInfo replicaPlacementInfo : task.proposal().newReplicas()) {
        newReplicas.add(replicaPlacementInfo.brokerId());
      }
      PartitionReassignment ongoingReassignment = ongoingReassignments.get(tp);
      switch (task.state()) {
        case ABORTING:
        case ABORTED:
        case DEAD:
          if (ongoingReassignment != null) {
            // A task in one of these states should not have a corresponding partition movement -- cancel it.
            newReassignments.put(tp, cancelReassignmentValue());
            LOG.debug("The ongoing reassignment {} will be cancelled for task {}.", ongoingReassignment, task);
          } else {
            // No action needed.
            LOG.debug("Task {} already has no corresponding replica reassignment.", task);
          }
          break;
        case COMPLETED:
          // No action needed.
          LOG.debug("Task {} has already been completed.", task);
          break;
        case IN_PROGRESS:
          if (ongoingReassignment != null) {
            // Ensure that the order of new replicas is the same as the requested task replica order
            if (!ongoingReassignment.addingReplicas().equals(newReplicas)) {
              newReassignments.put(tp, reassignmentValue(newReplicas));
              LOG.debug("Task {} will modify the ongoing reassignment {}.", task, ongoingReassignment);
            }
          } else {
            // No need to check whether the topic exists or being deleted as the server response will indicate those.
            newReassignments.put(tp, reassignmentValue(newReplicas));
            LOG.debug("Task {} will be executed.", task);
          }
          break;
        default:
          throw new IllegalStateException(String.format("Unrecognized task state %s.", task.state()));
      }
    }

    return newReassignments.isEmpty() ? null : adminClient.alterPartitionReassignments(newReassignments);
  }
}
