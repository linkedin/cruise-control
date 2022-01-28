/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor

import java.util.Properties
import kafka.zk.AdminZkClient
import org.slf4j.{Logger, LoggerFactory}

/**
 * This class is a Java interface wrapper of open source ReassignPartitionCommand. This class is needed because
 * scala classes and Java classes are not compatible.
 */
object ExecutorUtils {
  val LOG: Logger = LoggerFactory.getLogger(ExecutorUtils.getClass.getName)

  def changeBrokerConfig(adminZkClient: AdminZkClient, brokerId: Int, config: Properties): Unit = {
    adminZkClient.changeBrokerConfig(Some(brokerId), config)
  }

  def changeTopicConfig(adminZkClient: AdminZkClient, topic: String, config: Properties): Unit = {
    adminZkClient.changeTopicConfig(topic, config)
  }
}
