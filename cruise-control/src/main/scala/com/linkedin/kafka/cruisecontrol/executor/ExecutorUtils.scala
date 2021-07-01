/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor

import java.util
import java.util.Properties

import kafka.admin.PreferredReplicaLeaderElectionCommand
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * This class is a Java interface wrapper of open source ReassignPartitionCommand. This class is needed because
 * scala classes and Java classes are not compatible.
 */
object ExecutorUtils {
  val LOG: Logger = LoggerFactory.getLogger(ExecutorUtils.getClass.getName)

  def executePreferredLeaderElection(kafkaZkClient: KafkaZkClient, tasks: java.util.List[ExecutionTask]) {
    val partitionsToExecute = tasks.asScala.map(task =>
      new TopicPartition(task.proposal.topic, task.proposal.partitionId)).toSet
    val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(kafkaZkClient, partitionsToExecute)
    preferredReplicaElectionCommand.moveLeaderToPreferredReplica()
  }

  def ongoingLeaderElection(kafkaZkClient: KafkaZkClient): util.Set[TopicPartition] = {
    setAsJavaSet(kafkaZkClient.getPreferredReplicaElection)
  }

  def changeBrokerConfig(adminZkClient: AdminZkClient, brokerId: Int, config: Properties): Unit = {
    adminZkClient.changeBrokerConfig(Some(brokerId), config)
  }

  def changeTopicConfig(adminZkClient: AdminZkClient, topic: String, config: Properties): Unit = {
    adminZkClient.changeTopicConfig(topic, config)
  }
}
