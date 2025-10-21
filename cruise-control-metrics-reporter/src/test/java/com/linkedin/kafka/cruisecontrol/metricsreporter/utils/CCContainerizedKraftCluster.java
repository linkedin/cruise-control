/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.types.Password;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.MountableFile;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG;

/**
 * The {@code CCContainerizedKraftCluster} class creates a containerized KRaft Kafka cluster used for testing purposes.
 *
 * <p>This configuration sets up multiple listeners for different internal and external communication paths.
 * <ul>
 *   <li><b>CONTROLLER://0.0.0.0:9094</b>: Used for KRaft controller communication.</li>
 *   <li><b>INTERNAL://0.0.0.0:9095</b>: Used for client communication within the container network.</li>
 *   <li><b>EXTERNAL://0.0.0.0:9096</b>: Used for client communication outside the container network.</li>
 * </ul>
 */
public class CCContainerizedKraftCluster implements Startable {
  private static final String KAFKA_IMAGE = System.getenv().getOrDefault("KAFKA_IMAGE", "apache/kafka:3.9.1");
  /**
   * Determines the hostname used by containers to connect to services running on the host machine.
   * Required for CI environments like CircleCI, where the Docker executor relies on a specific hostname
   * for the CircleCI job to communicate with TestContainers services.
   */
  private static final String CONTAINER_HOST = System.getenv().getOrDefault("CONTAINER_HOST", "localhost");
  /**
   * Selects or creates the Docker network used for TestContainers communication.
   * Required for CI environments like CircleCI, where the Docker executor requires sharing the Docker network
   * to allow the CircleCI job to communicate with TestContainers services.
   */
  private static final Network NETWORK = System.getenv("NETWORK_NAME") != null
    ? Network.builder().id(System.getenv("NETWORK_NAME")).build()
    : Network.newNetwork();

  private static final String NETWORK_ALIAS_PREFIX = "broker";
  public static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";
  public static final String INTERNAL_LISTENER_NAME = "INTERNAL";
  public static final String EXTERNAL_LISTENER_NAME = "EXTERNAL";

  private static final int CONTAINER_CONTROLLER_LISTENER_PORT = 9094;
  public static final int CONTAINER_INTERNAL_LISTENER_PORT = 9095;
  private static final int CONTAINER_EXTERNAL_LISTENER_PORT = 9096;

  private final List<KafkaContainer> _brokers;
  private final String _bootstrapServers;

  public CCContainerizedKraftCluster(int numOfBrokers, List<Map<Object, Object>> brokerConfigs, Properties adminClientProps) {
    if (numOfBrokers <= 0) {
      throw new IllegalArgumentException("numOfBrokers '" + numOfBrokers + "' must be greater than 0");
    }

    String clusterId = Uuid.randomUuid().toString();
    String controllerQuorumVoters = getControllerQuorumVoters(numOfBrokers);
    List<Integer> containerHostPorts = getContainerHostPorts(numOfBrokers);
    String listeners = String.join(",",
      CONTROLLER_LISTENER_NAME + "://0.0.0.0:" + CONTAINER_CONTROLLER_LISTENER_PORT,
      INTERNAL_LISTENER_NAME + "://0.0.0.0:" + CONTAINER_INTERNAL_LISTENER_PORT,
      EXTERNAL_LISTENER_NAME + "://0.0.0.0:" + CONTAINER_EXTERNAL_LISTENER_PORT
    );
    this._bootstrapServers = generateBootstrapServersList(numOfBrokers, containerHostPorts);

    /*
     * Ideally, we would construct the admin client properties directly in this constructor. However, due to the current
     * inheritance structure in the test classes, we rely on externally provided properties to preserve required security
     * configurations. Cloning and augmenting the provided properties here is the simplest and safest way to handle this
     * without breaking existing test setups.
     */
    Properties adminClientPropsWithBootstrapAddress = new Properties();
    adminClientPropsWithBootstrapAddress.putAll(adminClientProps);
    adminClientPropsWithBootstrapAddress.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);

    this._brokers =
      IntStream
        .range(0, numOfBrokers)
        .mapToObj(brokerNum -> {

          Map<Object, Object> brokerConfig = brokerConfigs.get(brokerNum);
          brokerConfig.put("listeners", listeners);
          brokerConfig.put("node.id", brokerNum + "");
          brokerConfig.put("process.roles", "broker,controller");
          brokerConfig.put("controller.quorum.voters", controllerQuorumVoters);

          // TestContainers automatically sets `inter.broker.listener.name` so we must disable `security.inter.broker.protocol`
          // https://kafka.apache.org/documentation/#brokerconfigs_inter.broker.listener.name
          brokerConfig.put("inter.broker.listener.name", INTERNAL_LISTENER_NAME);
          brokerConfig.remove("security.inter.broker.protocol");

          String networkAlias = String.format("%s-%d", NETWORK_ALIAS_PREFIX, brokerNum);
          String metricsTopic = (String) brokerConfig.get(CRUISE_CONTROL_METRICS_TOPIC_CONFIG);
          int containerHostPort = containerHostPorts.get(brokerNum);

          KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_IMAGE) {
            // Overridden to allow specifying listeners in `advertised.listeners` list.
            // Tracking issue in TestContainer's Kafka module here: https://github.com/testcontainers/testcontainers-java/issues/10718
            @Override
            protected void containerIsStarting(InspectContainerResponse containerInfo) {
              String advertisedListeners = String.join(",",
                INTERNAL_LISTENER_NAME + "://" + containerInfo.getConfig().getHostName() + ":" + CONTAINER_INTERNAL_LISTENER_PORT,
                EXTERNAL_LISTENER_NAME + "://" + CONTAINER_HOST + ":" + containerHostPort);

              String command = String.format("#!/bin/bash%n"
                + "export KAFKA_ADVERTISED_LISTENERS=%s%n"
                + "/etc/kafka/docker/run%n", advertisedListeners);
              copyFileToContainer(Transferable.of(command, 0777), "/tmp/testcontainers_start.sh");
            }
          }
            .withNetwork(NETWORK)
            .withNetworkAliases(networkAlias)
            .withExposedPorts(CONTAINER_EXTERNAL_LISTENER_PORT)
            .withEnv("CLUSTER_ID", clusterId)
            // Uncomment the following line when debugging Kafka cluster problems.
            //.withLogConsumer(outputFrame -> System.out.print(networkAlias + " | " + outputFrame.getUtf8String()))
            // Uncomment the following line when debugging SSL connection problems.
            //.withEnv("KAFKA_OPTS", "-Djavax.net.debug=ssl,handshake")
            .waitingFor(new BrokerWaitStrategy(brokerNum, metricsTopic, adminClientPropsWithBootstrapAddress)
              .withStartupTimeout(Duration.ofMinutes(1))
            );
          kafkaContainer.setPortBindings(List.of(containerHostPort + ":" + CONTAINER_EXTERNAL_LISTENER_PORT));

          overrideBrokerConfig(kafkaContainer, brokerConfig);

          // Mount metrics reporter and copy generated certs into container
          setupContainerResources(kafkaContainer, brokerConfig);

          return kafkaContainer;
        })
        .collect(Collectors.toList());
  }

  private String getControllerQuorumVoters(int numOfBrokers) {
    return IntStream
      .range(0, numOfBrokers)
      .mapToObj(brokerNum -> String.format("%d@%s-%d:%d", brokerNum, NETWORK_ALIAS_PREFIX, brokerNum, CONTAINER_CONTROLLER_LISTENER_PORT))
      .collect(Collectors.joining(","));
  }

  private List<Integer> getContainerHostPorts(int numOfBrokers) {
    return IntStream.range(0, numOfBrokers)
      .mapToObj(i -> CCKafkaTestUtils.findLocalPort())
      .collect(Collectors.toList());
  }

  private String generateBootstrapServersList(int numOfBrokers, List<Integer> containerHostPorts) {
    return IntStream.range(0, numOfBrokers)
      .mapToObj(i -> String.format("%s:%s", CONTAINER_HOST, containerHostPorts.get(i)))
      .collect(Collectors.joining(","));
  }

  private void copyCertToContainer(KafkaContainer container, Map<Object, Object> config, String key) {
    if (config.containsKey(key)) {
      String path = config.get(key).toString();
      container.withCopyToContainer(MountableFile.forHostPath(path, 0644), path);
    }
  }

  private void setupContainerResources(KafkaContainer kafkaContainer, Map<Object, Object> brokerConfig) {
    Path libsDir = Paths.get("build", "libs").toAbsolutePath().normalize();

    try {
      Path jarPath = Files.list(libsDir)
        .filter(path -> path.getFileName().toString().startsWith("cruise-control-metrics-reporter"))
        .filter(path -> path.getFileName().toString().endsWith(".jar"))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Cruise Control Metrics Reporter jar not found in: " + libsDir));

      kafkaContainer.withCopyToContainer(
        MountableFile.forHostPath(jarPath.toString(), 0755),
        "/opt/kafka/libs/cruise-control-metrics-reporter.jar"
      );

      copyCertToContainer(kafkaContainer, brokerConfig, "ssl.truststore.location");
      copyCertToContainer(kafkaContainer, brokerConfig, "ssl.keystore.location");
      copyCertToContainer(kafkaContainer, brokerConfig, "cruise.control.metrics.reporter.ssl.keystore.location");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to mount Kafka container resources", e);
    }
  }

  /**
   * Updates the broker configuration of the Kafka container.
   *
   * @param kafkaContainer The Kafka container to be updated.
   * @param brokerConfig A map of properties that that will be translated into environment variables which the TestContainers
   *                     scripts will use to override the default broker configuration.
   */
  public void overrideBrokerConfig(KafkaContainer kafkaContainer, Map<Object, Object> brokerConfig) {
    for (Map.Entry<Object, Object> entry : brokerConfig.entrySet()) {
      String key = String.valueOf(entry.getKey());
      Object rawValue = entry.getValue();
      String value;

      if (rawValue instanceof Collection) {
        value = String.join(",", ((Collection<?>) rawValue).stream()
          .map(Object::toString)
          .toArray(String[]::new));
      } else if (rawValue instanceof String[]) {
        value = String.join(",", (String[]) rawValue);
      } else if (rawValue instanceof Password) {
        value = ((Password) rawValue).value();
      } else if (rawValue instanceof String) {
        value = (String) rawValue;
      } else {
        value = String.valueOf(rawValue);
      }

      // Convert Kafka broker properties key to env var format: e.g., log.retention.hours -> KAFKA_LOG_RETENTION_HOURS
      String envKey = "KAFKA_" + key.toUpperCase().replace('.', '_');
      kafkaContainer.withEnv(envKey, value);
    }
  }

  /**
   * Returns list of KafkaContainer broker objects within the TestContainer Kafka cluster.
   *
   * @return List of KafkaContainer broker objects within the TestContainer Kafka cluster.
   */
  public List<KafkaContainer> getBrokers() {
    return this._brokers;
  }

  /**
   * Returns a comma-separated list of bootstrap server addresses that are reachable from container host.
   *
   * @return A comma-separated list of external bootstrap server addresses in the form {@code host:port}.
   */
  public String getExternalBootstrapAddress() {
    return _bootstrapServers;
  }

  @Override
  public void start() {
    _brokers.parallelStream().forEach(GenericContainer::start);
  }

  @Override
  public void stop() {
    this._brokers.stream().parallel().forEach(GenericContainer::close);
  }

  public static class BrokerWaitStrategy extends AbstractWaitStrategy {
    private final int _brokerId;
    private final String _metricsTopic;
    private final Properties _adminClientProps;

    public BrokerWaitStrategy(int brokerId, String metricsTopic, Properties adminClientProps) {
      this._brokerId = brokerId;
      this._metricsTopic = metricsTopic;
      this._adminClientProps = adminClientProps;
    }

    @Override
    protected void waitUntilReady() {
      long deadline = System.currentTimeMillis() + startupTimeout.toMillis();

      try (Admin adminClient = Admin.create(_adminClientProps)) {
        while (System.currentTimeMillis() < deadline) {
          try {
            DescribeClusterResult cluster = adminClient.describeCluster();
            boolean brokerOnline = cluster.nodes().get().stream().anyMatch(node -> node.id() == _brokerId);

            if (!brokerOnline) {
              Thread.sleep(500);
              continue;
            }

            adminClient.describeTopics(Collections.singleton(_metricsTopic)).allTopicNames().get(5, TimeUnit.SECONDS);
            return;

          } catch (InterruptedException e) {
            // Restore interrupt status.
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for broker to become ready", e);
          } catch (ExecutionException | TimeoutException e) {
            // Kafka broker is not ready yet, ignore and retry.
          }
        }

        throw new RuntimeException(String.format("Broker %d did not become ready within timeout of %d ms", _brokerId, startupTimeout.toMillis()));
      }
    }
  }
}
