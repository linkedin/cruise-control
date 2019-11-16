/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static org.junit.Assert.assertEquals;


public class CruiseControlMetricsReporterTest extends CCKafkaClientsIntegrationTestHarness {
  protected static final String TOPIC = "CruiseControlMetricsReporterTest";

  /**
   * Setup the unit test.
   */
  @Before
  public void setUp() {
    super.setUp();
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

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    int port = findLocalPort();
    props.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
    props.setProperty(KafkaConfig.ListenersProp(), "PLAINTEXT://127.0.0.1:" + port);
    props.setProperty(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                      "127.0.0.1:" + port);
    props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
    props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
    props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
    props.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
    props.setProperty(KafkaConfig.DefaultReplicationFactorProp(), "2");
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

  protected int findLocalPort() {
    int port = -1;
    while (port < 0) {
      try {
        ServerSocket socket = new ServerSocket(0);
        socket.setReuseAddress(true);
        port = socket.getLocalPort();
        try {
          socket.close();
        } catch (IOException e) {
          // Ignore IOException on close()
        }
      } catch (IOException ie) {
        // let it go.
      }
    }
    return port;
  }
}
