/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.KafkaTopicDescriptionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCContainerizedKraftCluster;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.kafka.KafkaContainer;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.DEFAULT_BOOTSTRAP_SERVERS_HOST;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.DEFAULT_BOOTSTRAP_SERVERS_PORT;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.getTopicDescription;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CruiseControlMetricsReporterTest extends CCKafkaClientsIntegrationTestHarness {
  private static final int NUM_OF_BROKERS = 2;
  protected static final String TOPIC = "CruiseControlMetricsReporterTest";
  protected static final String HOST = "127.0.0.1";
  protected CCContainerizedKraftCluster _cluster;
  protected List<Map<Object, Object>> _brokerConfigs;

  /**
   * Setup the unit test.
   */
  @Before
  public void setUp() {
    Properties adminClientProps = new Properties();
    setSecurityConfigs(adminClientProps, "admin");

    _brokerConfigs = buildBrokerConfigs();
    _cluster = new CCContainerizedKraftCluster(NUM_OF_BROKERS, _brokerConfigs, adminClientProps);
    _cluster.start();
    _bootstrapUrl = _cluster.getExternalBootstrapAddress();

    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    AtomicInteger failed = new AtomicInteger(0);
    try (Producer<String, String> producer = createProducer(props)) {
      for (int i = 0; i < 10; i++) {
        producer.send(new ProducerRecord<>("TestTopic", Integer.toString(i)), new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
              failed.incrementAndGet();
            }
          }
        });
      }
    }
    assertEquals(0, failed.get());
  }

  /**
   * Tear down the unit test.
   */
  @After
  public void tearDown() {
    if (_cluster != null) {
      _cluster.close();
    }
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    props.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
    props.setProperty(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
      HOST + ":" + CCContainerizedKraftCluster.CONTAINER_INTERNAL_LISTENER_PORT);
    props.put("listener.security.protocol.map", String.join(",",
      CCContainerizedKraftCluster.CONTROLLER_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.INTERNAL_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.EXTERNAL_LISTENER_NAME + ":PLAINTEXT"));
    props.setProperty(CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
    props.setProperty(CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
    props.setProperty("log.flush.interval.messages", "1");
    props.setProperty("offsets.topic.replication.factor", "1");
    props.setProperty("default.replication.factor", "2");
    return props;
  }

  @Test
  public void testReportingMetrics() {
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testReportingMetrics");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    setSecurityConfigs(props, "consumer");
    Consumer<String, CruiseControlMetric> consumer = new KafkaConsumer<>(props);

    consumer.subscribe(Collections.singleton(TOPIC));
    long startMs = System.currentTimeMillis();
    HashSet<Integer> expectedMetricTypes = new HashSet<>(Arrays.asList((int) ALL_TOPIC_BYTES_IN.id(),
                                                                       (int) ALL_TOPIC_BYTES_OUT.id(),
                                                                       (int) TOPIC_BYTES_IN.id(),
                                                                       (int) TOPIC_BYTES_OUT.id(),
                                                                       (int) PARTITION_SIZE.id(),
                                                                       (int) BROKER_CPU_UTIL.id(),
                                                                       (int) ALL_TOPIC_REPLICATION_BYTES_IN.id(),
                                                                       (int) ALL_TOPIC_REPLICATION_BYTES_OUT.id(),
                                                                       (int) ALL_TOPIC_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) ALL_TOPIC_FETCH_REQUEST_RATE.id(),
                                                                       (int) ALL_TOPIC_MESSAGES_IN_PER_SEC.id(),
                                                                       (int) TOPIC_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) TOPIC_FETCH_REQUEST_RATE.id(),
                                                                       (int) TOPIC_MESSAGES_IN_PER_SEC.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_RATE.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_RATE.id(),
                                                                       (int) BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT.id(),
                                                                       (int) BROKER_REQUEST_QUEUE_SIZE.id(),
                                                                       (int) BROKER_RESPONSE_QUEUE_SIZE.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_PRODUCE_TOTAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_PRODUCE_TOTAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_PRODUCE_LOCAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_PRODUCE_LOCAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_LOG_FLUSH_RATE.id(),
                                                                       (int) BROKER_LOG_FLUSH_TIME_MS_MAX.id(),
                                                                       (int) BROKER_LOG_FLUSH_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH.id(),
                                                                       (int) BROKER_PRODUCE_TOTAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_PRODUCE_TOTAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_PRODUCE_LOCAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_PRODUCE_LOCAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_LOG_FLUSH_TIME_MS_50TH.id(),
                                                                       (int) BROKER_LOG_FLUSH_TIME_MS_999TH.id()));
    Set<Integer> metricTypes = new HashSet<>();
    ConsumerRecords<String, CruiseControlMetric> records;
    while (metricTypes.size() < expectedMetricTypes.size() && System.currentTimeMillis() < startMs + 15000) {
      records = consumer.poll(Duration.ofMillis(10L));
      for (ConsumerRecord<String, CruiseControlMetric> record : records) {
        metricTypes.add((int) record.value().rawMetricType().id());
      }
    }
    assertEquals("Expected " + expectedMetricTypes + ", but saw " + metricTypes, expectedMetricTypes, metricTypes);
  }

  private TopicDescription waitForTopicMetadata(Admin adminClient,
                                                Duration timeout,
                                                Predicate<TopicDescription> condition)
    throws InterruptedException, TimeoutException {

    long deadline = System.currentTimeMillis() + timeout.toMillis();

    while (System.currentTimeMillis() < deadline) {
      try {
        TopicDescription topicDescription = getTopicDescription((AdminClient) adminClient, TOPIC);

        if (condition.test(topicDescription)) {
          return topicDescription;
        }
      } catch (KafkaTopicDescriptionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          throw new RuntimeException("Failed to describe topic: " + TOPIC, e);
        }
        // else ignore and retry
      }

      Thread.sleep(500);
    }

    throw new TimeoutException("Timeout waiting for topic metadata condition to be met: " + TOPIC);
  }

  @Test
  public void testUpdatingMetricsTopicConfig() throws InterruptedException, TimeoutException {
    Properties props = new Properties();
    setSecurityConfigs(props, "admin");
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    Admin adminClient = Admin.create(props);

    TopicDescription topicDescription = waitForTopicMetadata(adminClient, Duration.ofSeconds(30), td -> true);
    assertEquals(1, topicDescription.partitions().size());

    KafkaContainer broker = _cluster.getBrokers().get(0);

    // Shutdown broker
    broker.stop();

    // Change broker config
    Map<Object, Object> brokerConfig = _brokerConfigs.get(0);
    brokerConfig.put(CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
    brokerConfig.put(CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "2");
    brokerConfig.put(CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

    _cluster.overrideBrokerConfig(broker, brokerConfig);

    // Restart broker
    broker.start();

    // Wait for topic metadata configuration change to propagate
    int oldPartitionCount = topicDescription.partitions().size();
    TopicDescription newTopicDescription = waitForTopicMetadata(adminClient, Duration.ofSeconds(30),
      td -> td.partitions().size() != oldPartitionCount);

    assertEquals(2, newTopicDescription.partitions().size());
  }

  @Test
  public void testGetKafkaBootstrapServersConfigure() {
    // Test with a "listeners" config with a host
    Map<Object, Object> brokerConfig = buildBrokerConfigs().get(0);
    Map<String, Object> listenersMap = Collections.singletonMap("listeners", brokerConfig.get("listeners"));
    String bootstrapServers = CruiseControlMetricsReporter.getBootstrapServers(listenersMap);
    String urlParse = "\\[?([0-9a-zA-Z\\-%._:]*)]?:(-?[0-9]+)";
    Pattern urlParsePattern = Pattern.compile(urlParse);
    assertTrue(urlParsePattern.matcher(bootstrapServers).matches());
    assertEquals("localhost", bootstrapServers.split(":")[0]);

    // Test with a "listeners" config without a host in the first listener.
    String listeners = "SSL://:1234,PLAINTEXT://myhost:4321";
    listenersMap = Collections.singletonMap("listeners", listeners);
    bootstrapServers = CruiseControlMetricsReporter.getBootstrapServers(listenersMap);
    assertTrue(urlParsePattern.matcher(bootstrapServers).matches());
    assertEquals(DEFAULT_BOOTSTRAP_SERVERS_HOST, bootstrapServers.split(":")[0]);
    assertEquals("1234", bootstrapServers.split(":")[1]);

    // Test with "listeners" and "port" config together.
    listenersMap = new HashMap<>();
    listenersMap.put("listeners", listeners);
    listenersMap.put("port", "43");
    bootstrapServers = CruiseControlMetricsReporter.getBootstrapServers(listenersMap);
    assertTrue(urlParsePattern.matcher(bootstrapServers).matches());
    assertEquals(DEFAULT_BOOTSTRAP_SERVERS_HOST, bootstrapServers.split(":")[0]);
    assertEquals("43", bootstrapServers.split(":")[1]);

    // Test with null "listeners" and "port" config.
    bootstrapServers = CruiseControlMetricsReporter.getBootstrapServers(Collections.emptyMap());
    assertTrue(urlParsePattern.matcher(bootstrapServers).matches());
    assertEquals(DEFAULT_BOOTSTRAP_SERVERS_HOST, bootstrapServers.split(":")[0]);
    assertEquals(DEFAULT_BOOTSTRAP_SERVERS_PORT, bootstrapServers.split(":")[1]);
  }
}
