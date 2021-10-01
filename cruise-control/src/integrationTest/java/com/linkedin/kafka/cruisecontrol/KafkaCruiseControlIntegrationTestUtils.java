/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import kafka.server.KafkaConfig;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test util class.
 */
public final class KafkaCruiseControlIntegrationTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlIntegrationTestUtils.class);
  private static final Random RANDOM = new Random(0xDEADBEEF);
  private KafkaCruiseControlIntegrationTestUtils() {

  }
  /**
   * Create JSON mapping configuration
   * @return the mapping configuration
   */
  public static Configuration createJsonMappingConfig() {
    return Configuration.builder().jsonProvider(new JacksonJsonProvider())
      .mappingProvider(new JacksonMappingProvider()).build();
  }
  /**
   * Find a random open port
   * @return with the port number
   */
  public static Integer findRandomOpenPortOnAllLocalInterfaces() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void waitForConditionMeet(BooleanSupplier condition, int retries, Error retriesExceededException) {
    waitForConditionMeet(condition, retries, Duration.ofSeconds(4), retriesExceededException);
  }
  /**
   * Execute boolean condition until it returns true
   * @param condition the condition to evaluate
   * @param retries the number of retries
   * @param retryBackoff the milliseconds between retries
   * @param retriesExceededException the exception if we run out of retries
   */
  public static void waitForConditionMeet(BooleanSupplier condition, int retries, Duration retryBackoff,
                                    Error retriesExceededException) {
    int counter = 0;
    while (! (counter == retries)) {
      counter++;
      boolean conditionResult = false;
      try {
        conditionResult = condition.getAsBoolean();
      } catch (Exception e) {
        LOG.warn("Exception occured", e);
      }
      if (conditionResult) {
        return;
      } else {
        try {
          Thread.sleep(retryBackoff.toMillis());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (retriesExceededException != null) {
      throw retriesExceededException;
    }
  }
  
  public static Properties getDefaultProducerProperties(String bootstrapServers) {
    return getDefaultProducerProperties(null, bootstrapServers);
  }
  /**
   * Create default producer properties with string key and value serializer
   * @param overrides the overrides to use
   * @param bootstrapServers the bootstrap servers url
   * @return the created properties
   */
  public static Properties getDefaultProducerProperties(Properties overrides, String bootstrapServers) {
    Properties result = new Properties();

    // populate defaults
    result.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    result.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getCanonicalName());
    result.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getCanonicalName());
    // apply overrides
    if (overrides != null) {
      result.putAll(overrides);
    }
    return result;
  }
  /**
   * Call cruise control REST API 
   * @param serverUrl the server base URL
   * @param path the path with get parameters
   * @return the response body as string
   */
  public static String callCruiseControl(String serverUrl, String path) {
    try {
      HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(serverUrl)
          .resolve(path).toURL().openConnection();
      String responseMessage =
          IOUtils.toString(stateEndpointConnection.getInputStream(), Charset.defaultCharset());
      return responseMessage;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create broker properties with metric reporter configuration
   * @return the created properties
   */
  public static Map<Object, Object> createBrokerProps() {
    Map<Object, Object> props = new HashMap<>();
    props.put("metric.reporters",
        "com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter");
    StringJoiner csvJoiner = new StringJoiner(",");
    csvJoiner.add(SecurityProtocol.PLAINTEXT.name + "://localhost:"
        + KafkaCruiseControlIntegrationTestUtils.findRandomOpenPortOnAllLocalInterfaces());
    props.put(KafkaConfig.ListenersProp(), csvJoiner.toString());
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
    return props;
  }
  /**
   * Create basic Cruise Control overrides for integration tests.
   * @return the configuration
   */
  public static Map<String, Object> ccConfigOverrides() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AnomalyDetectorConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
        "com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomalyFinder");
    configs.put(AnomalyDetectorConfig.TOPIC_ANOMALY_FINDER_CLASSES_CONFIG,
        "com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder");
    configs.put(AnomalyDetectorConfig.ANOMALY_NOTIFIER_CLASS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier");
    configs.put(AnomalyDetectorConfig.RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG, "true");

    configs.put(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler");
    configs.put(MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG, "36000");
    configs.put(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, "36000");
    configs.put(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, "3");

    configs.put(KafkaSampleStore.PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, "2");
    configs.put(KafkaSampleStore.BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, "2");
    configs.put(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, "true");
    return configs;
  }
  
  public static void produceRandomDataToTopic(String topicName, int messageSize, Properties producerConfig) {
    produceRandomDataToTopic(topicName, 1, messageSize, producerConfig);
  }
  /**
   * Produce random data to a kafka topic
   * @param topicName the topic name
   * @param messageCount the amount of the messages
   * @param messageSize the size of a message
   * @param producerConfig the configuration
   */
  public static void produceRandomDataToTopic(String topicName, int messageCount, int messageSize, Properties producerConfig) {
    if (messageSize > 0 && messageCount > 0) {
      try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
        byte[] randomRecords = new byte[messageSize];
        for (int i = 0; i < messageCount; i++) {
          RANDOM.nextBytes(randomRecords);
          producer.send(new ProducerRecord<>(topicName, Arrays.toString(randomRecords))).get();
          producer.flush();
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
