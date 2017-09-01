/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import com.linkedin.kafka.cruisecontrol.testutils.AbstractKafkaIntegrationTestHarness;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.ALL_TOPIC_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.ALL_TOPIC_BYTES_OUT;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.ALL_TOPIC_FETCH_REQUEST_RATE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.ALL_TOPIC_MESSAGES_IN_PER_SEC;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.ALL_TOPIC_PRODUCE_REQUEST_RATE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.BROKER_CONSUMER_FETCH_REQUEST_RATE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.BROKER_CPU_UTIL;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.BROKER_FOLLOWER_FETCH_REQUEST_RATE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.BROKER_PRODUCE_REQUEST_RATE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.PARTITION_SIZE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.TOPIC_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.TOPIC_BYTES_OUT;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.TOPIC_FETCH_REQUEST_RATE;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.TOPIC_MESSAGES_IN_PER_SEC;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricType.TOPIC_PRODUCE_REQUEST_RATE;
import static org.junit.Assert.assertEquals;


public class CruiseControlMetricsReporterTest extends AbstractKafkaIntegrationTestHarness {
  private static final String TOPIC = "CruiseControlMetricsReporterTest";
  @Before
  public void setUp() {
    super.setUp();
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    Producer<byte[], byte[]> producer = new KafkaProducer<>(props);
    AtomicInteger failed = new AtomicInteger(0);
    for (int i = 0; i < 10; i++) {
      producer.send(new ProducerRecord<>("TestTopic", Integer.toString(i).getBytes(StandardCharsets.UTF_8)), new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          if (e != null) {
            failed.incrementAndGet();
          }
        }
      });
    }
    producer.close();
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
    props.setProperty(KafkaConfig.MetricReporterClassesProp(), CruiseControlMetricsReporter.class.getName());
    props.setProperty(KafkaConfig.ListenersProp(), "PLAINTEXT://127.0.0.1:" + port);
    props.setProperty(CruiseControlMetricsReporter.CRUISE_CONTROL_METRICS_REPORTER_BOOTSTRAP_SERVERS,
                      "localhost:" + port);
    props.setProperty(CruiseControlMetricsReporter.CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS,
                      "100");
    props.setProperty(CruiseControlMetricsReporter.CRUISE_CONTROL_METRICS_TOPIC, TOPIC);
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
    Consumer<String, CruiseControlMetric> consumer = new KafkaConsumer<>(props);

    ConsumerRecords<String, CruiseControlMetric> records = ConsumerRecords.empty();
    consumer.subscribe(Collections.singletonList(TOPIC));
    long startMs = System.currentTimeMillis();
    Set<Integer> metricTypes = new HashSet<>();
    while (metricTypes.size() < 15 && System.currentTimeMillis() < startMs + 15000) {
      records = consumer.poll(10);
      for (ConsumerRecord<String, CruiseControlMetric> record : records) {
        metricTypes.add((int) record.value().metricType().id());
      }
    }
    HashSet<Integer> expectedMetricTypes = new HashSet<>(Arrays.asList((int) ALL_TOPIC_BYTES_IN.id(),
                                                                       (int) ALL_TOPIC_BYTES_OUT.id(),
                                                                       (int) TOPIC_BYTES_IN.id(),
                                                                       (int) TOPIC_BYTES_OUT.id(),
                                                                       (int) PARTITION_SIZE.id(),
                                                                       (int) BROKER_CPU_UTIL.id(),
                                                                       (int) ALL_TOPIC_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) ALL_TOPIC_FETCH_REQUEST_RATE.id(),
                                                                       (int) ALL_TOPIC_MESSAGES_IN_PER_SEC.id(),
                                                                       (int) TOPIC_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) TOPIC_FETCH_REQUEST_RATE.id(),
                                                                       (int) TOPIC_MESSAGES_IN_PER_SEC.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_RATE.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_RATE.id()));
    assertEquals("Expected to see " + expectedMetricTypes + ", but only see " + metricTypes, metricTypes, expectedMetricTypes);
  }

  private int findLocalPort() {
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
