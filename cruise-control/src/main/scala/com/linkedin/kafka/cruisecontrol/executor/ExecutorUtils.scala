/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor

import java.util

import kafka.admin.PreferredReplicaLeaderElectionCommand
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
 * This class is a Java interface wrapper of open source ReassignPartitionCommand. This class is needed because
 * scala classes and Java classes are not compatible.
 */
object ExecutorUtils {
  val LOG: Logger = LoggerFactory.getLogger(ExecutorUtils.getClass.getName)

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
        val oldReplicas = asScalaBuffer(task.proposal.oldReplicas()).map(_.toInt)
        val newReplicas = asScalaBuffer(task.proposal().newReplicas()).map(_.toInt)

        val inProgressReplicasOpt = newReplicaAssignment.get(tp)
        var addTask = true
        val replicasToWrite = inProgressReplicasOpt match {
          case Some(inProgressReplicas) =>
            if (task.state() == ExecutionTask.State.ABORTING) {
              oldReplicas
            } else if (task.state() == ExecutionTask.State.DEAD
              || task.state() == ExecutionTask.State.ABORTED
              || task.state() == ExecutionTask.State.COMPLETED) {
              addTask = false
              Seq.empty
            } else if (task.state() == ExecutionTask.State.IN_PROGRESS) {
              if (!newReplicas.equals(inProgressReplicas)) {
                throw new RuntimeException(s"The provided new replica list $newReplicas" +
                  s"is different from the in progress replica list $inProgressReplicas for $tp")
              }
              newReplicas
            } else {
              throw new IllegalStateException(s"Should never be here, the state is ${task.state()}")
            }
          case None =>
            if (task.state() == ExecutionTask.State.ABORTED
              || task.state() == ExecutionTask.State.DEAD
              || task.state() == ExecutionTask.State.ABORTING
              || task.state() == ExecutionTask.State.COMPLETED) {
              LOG.warn(s"No need to abort tasks $task because the partition is not in reassignment")
              addTask = false
              Seq.empty
            } else {
              // verify with current assignment
              val currentReplicaAssignment = zkUtils.getReplicasForPartition(topic, partition)
              if (currentReplicaAssignment.isEmpty) {
                LOG.warn(s"The partitionId $partition does not exist.")
                addTask = false
                Seq.empty
              } else {
                // we are not verifying the old replicas because the we may be reexecuting a task,
                // in which case the replica list could be different from the old replicas.
                newReplicas
              }
            }
        }
        if (addTask)
          newReplicaAssignment += (tp -> replicasToWrite)
      })

      // We do not use the ReassignPartitionsCommand here because we want to have incremental partition movement.
      if (newReplicaAssignment.nonEmpty)
        zkUtils.updatePartitionReassignmentData(newReplicaAssignment)
    }
  }

  def executePreferredLeaderElection(zkUtils: ZkUtils, tasks: java.util.List[ExecutionTask]) {
    val partitionsToExecute = tasks.map(task =>
      TopicAndPartition(task.proposal.topic, task.proposal.partitionId)).toSet

    val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(zkUtils, partitionsToExecute)
    preferredReplicaElectionCommand.moveLeaderToPreferredReplica()
  }

  def partitionsBeingReassigned(zkUtils: ZkUtils): util.Set[TopicPartition] = {
    setAsJavaSet(zkUtils.getPartitionsBeingReassigned().keys.map(tap => new TopicPartition(tap.topic, tap.partition)).toSet)
  }

  def ongoingLeaderElection(zkUtils: ZkUtils): util.Set[TopicPartition] = {
    setAsJavaSet(zkUtils.getPartitionsUndergoingPreferredReplicaElection()
                        .map(tap => new TopicPartition(tap.topic, tap.partition)).toSet)
  }

  def newAssignmentForPartition(zkUtils: ZkUtils, tp : TopicPartition): java.util.List[Integer] = {
    val inProgressReassignment =
      zkUtils.getPartitionsBeingReassigned().getOrElse(TopicAndPartition(tp.topic(), tp.partition()),
      throw new NoSuchElementException(s"Partition $tp is not being reassigned."))

    seqAsJavaList(inProgressReassignment.newReplicas.map(i => i : java.lang.Integer))
  }

  def currentReplicasForPartition(zkUtils: ZkUtils, tp: TopicPartition): java.util.List[java.lang.Integer] = {
    seqAsJavaList(zkUtils.getReplicasForPartition(tp.topic(), tp.partition()).map(i => i : java.lang.Integer))
  }
}
