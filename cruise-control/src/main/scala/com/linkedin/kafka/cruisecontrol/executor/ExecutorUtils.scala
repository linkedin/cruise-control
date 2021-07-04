/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor

import java.util
import java.util.Properties

import kafka.admin.PreferredReplicaLeaderElectionCommand
import kafka.zk.{AdminZkClient, KafkaZkClient, ZkVersion}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.javaapi.CollectionConverters

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
   * @param kafkaZkClient  the KafkaZkClient class to use for partition reassignment.
   * @param tasksToExecute Replica reassignment tasks to be executed.
   * @deprecated Use com.linkedin.kafka.cruisecontrol.executor.ExecutionUtils#submitReplicaReassignmentTasks() instead.
   */
  def executeReplicaReassignmentTasks(kafkaZkClient: KafkaZkClient,
                                      tasksToExecute: java.util.List[ExecutionTask]): Unit = {
    if (tasksToExecute != null && !tasksToExecute.isEmpty) {
      val inProgressReplicaReassignment: scala.collection.Map[TopicPartition, scala.collection.Seq[Int]] = kafkaZkClient.getPartitionReassignment
      // Add the partition being assigned to the newReplicaAssignment because we are going to add the new
      // reassignment together.
      val newReplicaAssignment: scala.collection.mutable.Map[TopicPartition, scala.collection.Seq[Int]] =
      scala.collection.mutable.Map.from[TopicPartition, scala.collection.Seq[Int]](inProgressReplicaReassignment.toSeq)
      CollectionConverters.asScala(tasksToExecute).toSeq.map({ task =>
        val tp = task.proposal().topicPartition()
        val oldReplicas = CollectionConverters.asScala(task.proposal.oldReplicas()).map(_.brokerId.toInt)
        val newReplicas = CollectionConverters.asScala(task.proposal().newReplicas()).map(_.brokerId.toInt)

        val inProgressReplicasOpt = newReplicaAssignment.get(tp)
        var addTask = true
        val replicasToWrite = inProgressReplicasOpt match {
          case Some(inProgressReplicas) =>
            if (task.state() == ExecutionTaskState.ABORTING) {
              oldReplicas
            } else if (task.state() == ExecutionTaskState.DEAD
              || task.state() == ExecutionTaskState.ABORTED
              || task.state() == ExecutionTaskState.COMPLETED) {
              addTask = false
              Seq.empty
            } else if (task.state() == ExecutionTaskState.IN_PROGRESS) {
              if (!newReplicas.equals(inProgressReplicas)) {
                throw new RuntimeException(s"The provided new replica list $newReplicas" +
                  s"is different from the in progress replica list $inProgressReplicas for $tp")
              }
              newReplicas
            } else {
              throw new IllegalStateException(s"Should never be here, the state is ${task.state()}")
            }
          case None =>
            if (task.state() == ExecutionTaskState.ABORTED
              || task.state() == ExecutionTaskState.DEAD
              || task.state() == ExecutionTaskState.ABORTING
              || task.state() == ExecutionTaskState.COMPLETED) {
              LOG.warn(s"No need to abort tasks $task because the partition is not in reassignment")
              addTask = false
              Seq.empty
            } else {
              // verify with current assignment
              val currentReplicaAssignment = kafkaZkClient.getReplicasForPartition(tp)
              if (currentReplicaAssignment.isEmpty) {
                LOG.warn(s"The partition $tp does not exist.")
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
        kafkaZkClient.setOrCreatePartitionReassignment(newReplicaAssignment, ZkVersion.MatchAnyVersion)
    }
  }

  def executePreferredLeaderElection(kafkaZkClient: KafkaZkClient, tasks: java.util.List[ExecutionTask]): Unit = {
    val partitionsToExecute = CollectionConverters.asScala(tasks).map(task =>
      new TopicPartition(task.proposal.topic, task.proposal.partitionId)).toSet
    val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(kafkaZkClient, partitionsToExecute)
    preferredReplicaElectionCommand.moveLeaderToPreferredReplica()
  }

  /**
   * Retrieve the set of partitions that are currently being reassigned.
   *
   * @param kafkaZkClient the KafkaZkClient class to use for getting partition reassignment.
   * @return Set of partitions with ongoing reassignments.
   * @deprecated Use [[com.linkedin.kafka.cruisecontrol.executor.ExecutionUtils]]#partitionsBeingReassigned instead.
   */
  def partitionsBeingReassigned(kafkaZkClient: KafkaZkClient): util.Set[TopicPartition] = {
    CollectionConverters.asJava(kafkaZkClient.getPartitionReassignment.keys.toSet)
  }

  def ongoingLeaderElection(kafkaZkClient: KafkaZkClient): util.Set[TopicPartition] = {
    CollectionConverters.asJava(kafkaZkClient.getPreferredReplicaElection)
  }

  def newAssignmentForPartition(kafkaZkClient: KafkaZkClient, tp : TopicPartition): java.util.List[Integer] = {
    val inProgressReassignment =
      kafkaZkClient.getPartitionReassignment.getOrElse(new TopicPartition(tp.topic(), tp.partition()),
      throw new NoSuchElementException(s"Partition $tp is not being reassigned."))

    CollectionConverters.asJava(inProgressReassignment.map(i => i : java.lang.Integer))
  }

  def currentReplicasForPartition(kafkaZkClient: KafkaZkClient, tp: TopicPartition): java.util.List[java.lang.Integer] = {
    CollectionConverters.asJava(kafkaZkClient.getReplicasForPartition(new TopicPartition(tp.topic(), tp.partition())).map(i => i : java.lang.Integer))
  }

  def changeBrokerConfig(adminZkClient: AdminZkClient, brokerId: Int, config: Properties): Unit = {
    adminZkClient.changeBrokerConfig(Some(brokerId), config)
  }

  def changeTopicConfig(adminZkClient: AdminZkClient, topic: String, config: Properties): Unit = {
    adminZkClient.changeTopicConfig(topic, config)
  }

  def getAllLiveBrokerIdsInCluster(kafkaZkClient: KafkaZkClient): java.util.List[java.lang.Integer] = {
    CollectionConverters.asJava(kafkaZkClient.getAllBrokersInCluster.map(_.id : java.lang.Integer))
  }
}
