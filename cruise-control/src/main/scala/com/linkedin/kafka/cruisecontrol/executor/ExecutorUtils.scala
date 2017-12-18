/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor

import com.linkedin.kafka.cruisecontrol.common.BalancingAction
import kafka.admin.{PreferredReplicaLeaderElectionCommand, ReassignPartitionsCommand}
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * This class is a Java interface wrapper of open source ReassignPartitionCommand. This class is needed because
 * scala classes and Java classes are not compatible.
 */
object ExecutorUtils {
  val LOG = LoggerFactory.getLogger(ExecutorUtils.getClass.getName)

  /**
   * Add a list of replica reassignment tasks to execute. Replica reassignment indicates tasks that (1) relocate a replica
   * within the cluster, (2) introduce a new replica to the cluster (3) remove an existing replica from the cluster.
   *
   * @param zkUtils the ZkUtils class to use for partition reassignment.
   * @param reassignmentTasks Replica reassignment tasks to be executed.
   */
  def executeReplicaReassignmentTasks(zkUtils: ZkUtils,
                                      reassignmentTasks: java.util.List[ExecutionTask]) {
    if (reassignmentTasks != null && !reassignmentTasks.isEmpty) {
      val inProgressReplicaReassignment = zkUtils.getPartitionsBeingReassigned()
      // Add the partition being assigned to the newReplicaAssignment because we are going to add the new
      // reassignment together.
      val newReplicaAssignment = scala.collection.mutable.Map(inProgressReplicaReassignment.map { case (tp, context) =>
        tp -> context.newReplicas
      }.toSeq: _*)
      reassignmentTasks.foreach({ task =>
        val topic = task.proposal.topic
        val partition = task.proposal.partitionId
        val tp = TopicAndPartition(topic, partition)
        val sourceBroker = task.sourceBrokerId()
        val destinationBroker = task.destinationBrokerId()

        val inProgressReplicasOpt = newReplicaAssignment.get(tp)
        var addTask = true
        val newReplicas = inProgressReplicasOpt match {
          case Some(inProgressReplicas) =>
            if (task.healthiness() == ExecutionTask.Healthiness.ABORTED) {
              // verify with in progress assignment
              if (!inProgressReplicas.contains(destinationBroker))
                throw new RuntimeException(s"Broker $destinationBroker is not being assigned as a replica for [$topic, $partition] in $inProgressReplicas")
              // It is possible that the source broker is also dead in cases of double failures.
              // We handle this case in the executor, so the task is never propagated to here.
              if (sourceBroker != null)
                inProgressReplicas.filter(_ != destinationBroker) :+ sourceBroker.toInt
              else
                inProgressReplicas.filter(_ != destinationBroker)
            } else if (task.healthiness() == ExecutionTask.Healthiness.DEAD) {
              addTask = false
              Seq.empty
            } else if (task.healthiness() == ExecutionTask.Healthiness.NORMAL) {
              // verify with in progress assignment
              if (task.proposal.balancingAction() != BalancingAction.REPLICA_DELETION && inProgressReplicas.contains(destinationBroker))
                throw new RuntimeException(s"Broker $destinationBroker is already being assigned as a replica for [$topic, $partition]")
              if (task.proposal.balancingAction() != BalancingAction.REPLICA_ADDITION && !inProgressReplicas.contains(sourceBroker))
                throw new RuntimeException(s"Broker $sourceBroker is not assigned as a replica in previous partition movement for [$topic, $partition]")
              if (destinationBroker != null)
                (inProgressReplicas :+ destinationBroker.toInt).filter(_ != sourceBroker)
              else
                inProgressReplicas.filter(_ != sourceBroker)
            } else {
              throw new IllegalStateException("Should never be here")
            }
          case None =>
            if (task.healthiness() == ExecutionTask.Healthiness.ABORTED
              || task.healthiness() == ExecutionTask.Healthiness.DEAD) {
              LOG.warn(s"No need to abort tasks $task because the partition is not in reassignment")
              addTask = false
              Seq.empty
            } else {
              // verify with current assignment
              val currentReplicaAssignment = zkUtils.getReplicasForPartition(topic, partition)
              if (currentReplicaAssignment.isEmpty) {
                LOG.warn(s"The partition $partition does not exist.")
                addTask = false
                Seq.empty
              } else {
                if (task.proposal.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
                  if (currentReplicaAssignment.contains(destinationBroker) && !currentReplicaAssignment.contains(sourceBroker)) {
                    // Reassignment is done already.
                    addTask = false
                    Seq.empty
                  } else {
                    // Get new replicas for replica movement.
                    getNewReplicasForReplicaMovement(currentReplicaAssignment, task)
                  }
                } else if (task.proposal.balancingAction() == BalancingAction.REPLICA_ADDITION) {
                  if (currentReplicaAssignment.contains(destinationBroker)) {
                    // Reassignment is done already.
                    addTask = false
                    Seq.empty
                  } else {
                    // Get new replicas for replica addition.
                    getNewReplicasForReplicaAddition(currentReplicaAssignment, task)
                  }
                } else if (task.proposal.balancingAction() == BalancingAction.REPLICA_DELETION) {
                  if (!currentReplicaAssignment.contains(sourceBroker)) {
                    // Reassignment is done already.
                    addTask = false
                    Seq.empty
                  } else {
                    // Get new replicas for replica deletion.
                    getNewReplicasForReplicaDeletion(currentReplicaAssignment, task)
                  }
                } else {
                  throw new RuntimeException(s"Unexpected balancing action for proposal ${task.proposal} for [$topic, $partition]")
                }
              }
            }
        }
        if (addTask)
          newReplicaAssignment += (tp -> newReplicas)
      })

      // We do not use the ReassignPartitionsCommand here because we want to have incremental partition movement.
      if (newReplicaAssignment.nonEmpty)
        zkUtils.updatePartitionReassignmentData(newReplicaAssignment)
    }
  }

  def getNewReplicasForReplicaAddition(currentReplicaAssignment: Seq[Int], task: ExecutionTask): Seq[Int] = {
    // Source broker is guaranteed to be a null for replica addition.
    val destinationBroker = task.destinationBrokerId()
    currentReplicaAssignment :+ destinationBroker.toInt
  }

  def getNewReplicasForReplicaDeletion(currentReplicaAssignment: Seq[Int], task: ExecutionTask): Seq[Int] = {
    // Destination broker is guaranteed to be a null for replica deletion.
    val sourceBroker = task.sourceBrokerId()
    currentReplicaAssignment.filter(_ != sourceBroker)
  }

  def getNewReplicasForReplicaMovement(currentReplicaAssignment: Seq[Int], task: ExecutionTask): Seq[Int] = {
    // Both source and destination brokers are guaranteed to be non-null for replica movement.
    val sourceBroker = task.sourceBrokerId()
    val destinationBroker = task.destinationBrokerId()
    if (!currentReplicaAssignment.contains(destinationBroker)) {
      if (currentReplicaAssignment.contains(sourceBroker)) {
        // This is a normal movement.
        (currentReplicaAssignment :+ destinationBroker.toInt).filter(_ != sourceBroker)
      } else {
        throw new IllegalStateException(s"Unexpected proposal ${task.proposal}. Neither source nor destination broker" +
          s" is in the current replica for ${BalancingAction.REPLICA_MOVEMENT}. Current replica assignment: " +
          s"$currentReplicaAssignment.")
      }
    } else {
      // The destination broker is already in the list, we just need to filter out the source broker if it exists.
      currentReplicaAssignment.filter(_ != sourceBroker)
    }
  }

  def adjustReplicaOrderBeforeLeaderMovements(zkUtils: ZkUtils,
                                              tasks: java.util.List[ExecutionTask]) {
    val inProgressPartitionMovement = zkUtils.getPartitionsBeingReassigned()
    if (inProgressPartitionMovement.nonEmpty)
      throw new IllegalStateException("The partition movements should have finished before leader movements start.")

    val newReplicaAssignment = tasks.flatMap { task =>
      val topic = task.proposal.topic
      val partition = task.proposal.partitionId
      val tp = TopicAndPartition(topic, partition)
      val destinationBroker = task.destinationBrokerId().toInt

      val currentAssignment = zkUtils.getReplicasForPartition(topic, partition)
      if (currentAssignment.nonEmpty) {
        val replicasWithoutLeader = currentAssignment.filter(_ != destinationBroker)
        if (currentAssignment.size != replicasWithoutLeader.size + 1)
          throw new IllegalStateException(s"Current replicas $currentAssignment for $tp does not contain new " +
            s"leader $destinationBroker")
        val newReplicas = destinationBroker +: replicasWithoutLeader
        Some(tp -> newReplicas)
      } else {
        None
      }
    }.toMap

    if (newReplicaAssignment.nonEmpty) {
      val reassignPartitionCommand = new ReassignPartitionsCommand(zkUtils, newReplicaAssignment)
      if (!reassignPartitionCommand.reassignPartitions())
        throw new RuntimeException(s"partition assignment for $newReplicaAssignment failed because of ZK write failure")
    }
  }

  def executePreferredLeaderElection(zkUtils: ZkUtils,
                                     tasks: java.util.List[ExecutionTask]) {
    val partitionsToExecute = tasks.map(task =>
      TopicAndPartition(task.proposal.topic, task.proposal.partitionId)).toSet

    val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(zkUtils, partitionsToExecute)
    preferredReplicaElectionCommand.moveLeaderToPreferredReplica()
  }

  def partitionsBeingReassigned(zkUtils: ZkUtils) = {
    setAsJavaSet(zkUtils.getPartitionsBeingReassigned().keys.map(tap => new TopicPartition(tap.topic, tap.partition)).toSet)
  }

  def newAssignmentForPartition(zkUtils: ZkUtils, tp : TopicPartition): java.util.List[Integer] = {
    val inProgressReassignment =
      zkUtils.getPartitionsBeingReassigned().getOrElse(TopicAndPartition(tp.topic(), tp.partition()),
      throw new NoSuchElementException(s"Partition $tp is not being reassigned."))

    seqAsJavaList(inProgressReassignment.newReplicas.map(i => i : java.lang.Integer))
  }
}
