/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.linkedin.kafka.cruisecontrol.testutils

import java.io.File
import java.util
import java.util.Properties

import kafka.common.KafkaException
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.CoreUtils
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.auth.KafkaPrincipal

import scala.collection.mutable

/**
 * Kafka server integration test harness.
 * This is simply a copy from open source Apache Kafka code, we do this because java does not support trait,
 * we are making it abstract class so user java test class can extend it.
 */
abstract class AbstractKafkaServerTestHarness extends AbstractZookeeperTestHarness {
  var instanceConfigs: Seq[KafkaConfig] = null
  var servers: mutable.Buffer[KafkaServer] = null
  var brokerList: String = null
  var alive: Array[Boolean] = null
  val kafkaPrincipalType = KafkaPrincipal.USER_TYPE
  val setClusterAcl: Option[() => Unit] = None

  /**
   * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
   * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
   */
  def generateConfigs(): Seq[KafkaConfig]

  def configs: Seq[KafkaConfig] = {
    if (instanceConfigs == null)
      instanceConfigs = generateConfigs()
    instanceConfigs
  }

  def serverForId(id: Int) = servers.find(s => s.config.brokerId == id)

  def bootstrapUrl: String = brokerList

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT

  protected def trustStoreFile: Option[File] = None

  protected def saslProperties: Option[Properties] = None

  override def setUp() {
    super.setUp()
    if (configs.size <= 0)
      throw new KafkaException("Must supply at least one server config.")
    servers = configs.map(TestUtils.createServer(_)).toBuffer
    brokerList = TestUtils.getBrokerListStrFromServers(servers, securityProtocol)
    alive = new Array[Boolean](servers.length)
    util.Arrays.fill(alive, true)
    // We need to set a cluster ACL in some cases here
    // because of the topic creation in the setup of
    // IntegrationTestHarness. If we don't, then tests
    // fail with a cluster action authorization exception
    // when processing an update metadata request
    // (controller -> broker).
    //
    // The following method does nothing by default, but
    // if the test case requires setting up a cluster ACL,
    // then it needs to be implemented.
    setClusterAcl.foreach(_.apply())
  }

  override def tearDown() {
    if (servers != null) {
      servers.foreach(_.shutdown())
      servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    }
    super.tearDown()
  }

  /**
   * Pick a broker at random and kill it if it isn't already dead
   * Return the id of the broker killed
   */
  def killRandomBroker(): Int = {
    val index = TestUtils.random.nextInt(servers.length)
    if (alive(index)) {
      servers(index).shutdown()
      servers(index).awaitShutdown()
      alive(index) = false
    }
    index
  }

  /**
   * Restart any dead brokers
   */
  def restartDeadBrokers() {
    for (i <- 0 until servers.length if !alive(i)) {
      servers(i).startup()
      alive(i) = true
    }
  }
}
