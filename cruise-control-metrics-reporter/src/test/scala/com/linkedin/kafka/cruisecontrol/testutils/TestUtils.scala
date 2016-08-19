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
import java.nio.file.Files
import java.util.{Random, Properties}

import kafka.admin.{RackAwareMode, AdminUtils}
import kafka.server.{KafkaServer, KafkaConfig}
import kafka.utils.{ZkUtils, SystemTime, Time}
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.utils.Utils.formatAddress

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * This is a copy of Apache Kafka embedded zookeeper but removed the dependency on o.a.k.test.TestUtils dependency.
 */
object TestUtils {

  val IoTmpDir = System.getProperty("java.io.tmpdir")
  val random = new Random()
  /* 0 gives a random port; you can then retrieve the assigned port from the Socket object. */
  val RandomPort = 0

  /**
   * Create a kafka server instance with appropriate test settings
   * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
   *
   * @param config The configuration of the server
   */
  def createServer(config: KafkaConfig, time: Time = SystemTime): KafkaServer = {
    val server = new KafkaServer(config, time)
    server.startup()
    server
  }

  def getBrokerListStrFromServers(servers: Seq[KafkaServer], protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): String = {
    servers.map(s => formatAddress(s.config.hostName, s.boundPort(protocol))).mkString(",")
  }

  /**
   * Create a test config for the provided parameters.
   *
   * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
   */
  def createBrokerConfigs(numConfigs: Int,
                          zkConnect: String,
                          enableControlledShutdown: Boolean = true,
                          enableDeleteTopic: Boolean = false,
                          interBrokerSecurityProtocol: Option[SecurityProtocol] = None,
                          trustStoreFile: Option[File] = None,
                          enablePlaintext: Boolean = true,
                          enableSsl: Boolean = false,
                          enableSaslPlaintext: Boolean = false,
                          enableSaslSsl: Boolean = false,
                          rackInfo: Map[Int, String] = Map()): Seq[Properties] = {
    (0 until numConfigs).map { node =>
      createBrokerConfig(node, zkConnect, enableControlledShutdown, enableDeleteTopic, RandomPort,
        interBrokerSecurityProtocol, trustStoreFile, enablePlaintext = enablePlaintext, enableSsl = enableSsl,
        enableSaslPlaintext = enableSaslPlaintext, enableSaslSsl = enableSaslSsl, rack = rackInfo.get(node))
    }
  }

  def sslConfigs(mode: Mode, clientCert: Boolean, trustStoreFile: Option[File], certAlias: String): Properties = {

    val trustStore = trustStoreFile.getOrElse {
      throw new Exception("SSL enabled but no trustStoreFile provided")
    }

    val sslConfigs = TestSslUtils.createSslConfig(clientCert, true, mode, trustStore, certAlias)

    val sslProps = new Properties()
    sslConfigs.foreach { case (k, v) => sslProps.put(k, v)}
    sslProps
  }

  private def usesSslTransportLayer(securityProtocol: SecurityProtocol): Boolean = securityProtocol match {
    case SecurityProtocol.SSL | SecurityProtocol.SASL_SSL => true
    case _ => false
  }

  /**
   * Create a test config for the provided parameters.
   *
   * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
   */
  def createBrokerConfig(nodeId: Int, zkConnect: String,
                         enableControlledShutdown: Boolean = true,
                         enableDeleteTopic: Boolean = false,
                         port: Int = RandomPort,
                         interBrokerSecurityProtocol: Option[SecurityProtocol] = None,
                         trustStoreFile: Option[File] = None,
                         enablePlaintext: Boolean = true,
                         enableSaslPlaintext: Boolean = false, saslPlaintextPort: Int = RandomPort,
                         enableSsl: Boolean = false, sslPort: Int = RandomPort,
                         enableSaslSsl: Boolean = false, saslSslPort: Int = RandomPort, rack: Option[String] = None)
  : Properties = {

    def shouldEnable(protocol: SecurityProtocol) = interBrokerSecurityProtocol.fold(false)(_ == protocol)

    val protocolAndPorts = ArrayBuffer[(SecurityProtocol, Int)]()
    if (enablePlaintext || shouldEnable(SecurityProtocol.PLAINTEXT))
      protocolAndPorts += SecurityProtocol.PLAINTEXT -> port
    if (enableSsl || shouldEnable(SecurityProtocol.SSL))
      protocolAndPorts += SecurityProtocol.SSL -> sslPort
    if (enableSaslPlaintext || shouldEnable(SecurityProtocol.SASL_PLAINTEXT))
      protocolAndPorts += SecurityProtocol.SASL_PLAINTEXT -> saslPlaintextPort
    if (enableSaslSsl || shouldEnable(SecurityProtocol.SASL_SSL))
      protocolAndPorts += SecurityProtocol.SASL_SSL -> saslSslPort

    val listeners = protocolAndPorts.map { case (protocol, port) =>
      s"${protocol.name}://localhost:$port"
    }.mkString(",")

    val props = new Properties
    if (nodeId >= 0) props.put("broker.id", nodeId.toString)
    props.put("listeners", listeners)
    props.put("log.dir", TestUtils.tempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkConnect)
    props.put("replica.socket.timeout.ms", "1500")
    props.put("controller.socket.timeout.ms", "1500")
    props.put("controlled.shutdown.enable", enableControlledShutdown.toString)
    props.put("delete.topic.enable", enableDeleteTopic.toString)
    props.put("controlled.shutdown.retry.backoff.ms", "100")
    props.put("log.cleaner.dedupe.buffer.size", "2097152")
    rack.foreach(props.put("broker.rack", _))

    if (protocolAndPorts.exists { case (protocol, _) => usesSslTransportLayer(protocol)})
      props.putAll(sslConfigs(Mode.SERVER, clientCert = false, trustStoreFile, s"server$nodeId"))

    interBrokerSecurityProtocol.foreach { protocol =>
      props.put(KafkaConfig.InterBrokerSecurityProtocolProp, protocol.name)
    }

    props.put("port", port.toString)
    props
  }

  /**
   * Create a temporary directory
   */
  def tempDir(): File = {
    tempRelativeDir(IoTmpDir)
  }

  def tempTopic(): String = "testTopic" + random.nextInt(1000000)

  /**
   * Create a temporary relative directory
   */
  def tempRelativeDir(parent: String): File = {
    val parentFile = new File(parent)
    parentFile.mkdirs()
    val f = Files.createTempDirectory(parentFile.toPath, "kafka-").toFile
    f.deleteOnExit()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() = {
        Utils.delete(f)
      }
    })
    f
  }
}