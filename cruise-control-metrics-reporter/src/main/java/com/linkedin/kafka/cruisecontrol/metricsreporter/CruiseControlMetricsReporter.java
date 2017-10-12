/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricsUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.YammerMetricProcessor;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CruiseControlMetricsReporter implements MetricsReporter, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsReporter.class);
  private YammerMetricProcessor _yammerMetricProcessor;
  private Map<org.apache.kafka.common.MetricName, KafkaMetric> _interestedMetrics = new ConcurrentHashMap<>();
  private KafkaThread _metricsReporterRunner;
  private KafkaProducer<String, CruiseControlMetric> _producer;
  private String _cruiseControlMetricsTopic;
  private long _reportingIntervalMs;
  private int _brokerId;
  private long _lastReportingTime = System.currentTimeMillis();
  private int _numMetricSendFailure = 0;
  private volatile boolean _shutdown = false;

  @Override
  public void init(List<KafkaMetric> metrics) {
    for (KafkaMetric kafkaMetric : metrics) {
      addMetricIfInterested(kafkaMetric);
    }
    LOG.info("Added {} kafka metrics for cruise control metrics during initialization.", _interestedMetrics.size());
    _metricsReporterRunner = new KafkaThread("CruiseControlMetricsReporterRunner", this, true);
    _yammerMetricProcessor = new YammerMetricProcessor();
    _metricsReporterRunner.start();
  }

  @Override
  public void metricChange(KafkaMetric metric) {
    addMetricIfInterested(metric);
  }

  @Override
  public void metricRemoval(KafkaMetric metric) {
    _interestedMetrics.remove(metric.metricName());
  }

  @Override
  public void close() {
    LOG.info("Closing cruise control metrics reporter.");
    _shutdown = true;
    if (_metricsReporterRunner != null) {
      _metricsReporterRunner.interrupt();
    }
    if (_producer != null) {
      _producer.close(5, TimeUnit.SECONDS);
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Properties producerProps = CruiseControlMetricsReporterConfig.parseProducerConfigs(configs);
    if (!producerProps.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
      String port = (String) configs.get("port");
      String bootstrapServers = "localhost:" + (port == null ? "9092" : port);
      producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      LOG.info("Using default value of {} for {}", bootstrapServers,
               CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }
    if (!producerProps.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
      String securityProtocol = "PLAINTEXT";
      producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      LOG.info("Using default value of {} for {}", securityProtocol,
               CruiseControlMetricsReporterConfig.config(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }
    CruiseControlMetricsReporterConfig reporterConfig = new CruiseControlMetricsReporterConfig(configs, false);

    setIfAbsent(producerProps,
                ProducerConfig.CLIENT_ID_CONFIG,
                reporterConfig.getString(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.CLIENT_ID_CONFIG)));
    // Set batch.size and linger.ms to a big number to have better batching.
    setIfAbsent(producerProps, ProducerConfig.LINGER_MS_CONFIG, "30000");
    setIfAbsent(producerProps, ProducerConfig.BATCH_SIZE_CONFIG, "800000");
    setIfAbsent(producerProps, ProducerConfig.RETRIES_CONFIG, "5");
    setIfAbsent(producerProps, ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    setIfAbsent(producerProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    setIfAbsent(producerProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
    setIfAbsent(producerProps, ProducerConfig.ACKS_CONFIG, "all");
    _producer = new KafkaProducer<>(producerProps);

    _brokerId = Integer.parseInt((String) configs.get(KafkaConfig.BrokerIdProp()));

    _cruiseControlMetricsTopic = reporterConfig.getString(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG);
    _reportingIntervalMs = reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTING_INTERVAL_MS_CONFIG);
  }

  @Override
  public void run() {
    LOG.info("Starting cruise control metrics reporter with reporting interval of {} ms.", _reportingIntervalMs);
    try {
      while (!_shutdown) {
        long now = System.currentTimeMillis();
        LOG.debug("Reporting metrics for time {}.", now);
        try {
          if (now > _lastReportingTime + _reportingIntervalMs) {
            _numMetricSendFailure = 0;
            _lastReportingTime = now;
            reportYammerMetrics(now);
            reportKafkaMetrics(now);
            reportCpuUtils(now);
          }
          try {
            _producer.flush();
          } catch (InterruptException ie) {
            if (_shutdown) {
              LOG.info("Cruise control metric reporter is interrupted during flush due to shutdown request.");
            } else {
              throw ie;
            }
          }
        } catch (Exception e) {
          LOG.error("Got exception in cruise control metrics reporter", e);
        }
        // Log failures if there is any.
        if (_numMetricSendFailure > 0) {
          LOG.warn("Failed to send {} metrics for time {}", _numMetricSendFailure, now);
        }
        _numMetricSendFailure = 0;
        long nextReportTime = now + _reportingIntervalMs;
        LOG.debug("Reporting finished for time {} in {} ms. Next reporting time {}",
                  now, System.currentTimeMillis() - now, nextReportTime);
        while (!_shutdown && now < nextReportTime) {
          try {
            Thread.sleep(nextReportTime - now);
          } catch (InterruptedException ie) {
            // let it go
          }
          now = System.currentTimeMillis();
        }
      }
    } finally {
      LOG.info("Cruise control metrics reporter exited.");
    }
  }

  /**
   * Send a CruiseControlMetric to the Kafka topic.
   * @param ccm the cruise control metric to send.
   */
  public void sendCruiseControlMetric(CruiseControlMetric ccm) {
    // Use topic name as key if existing so that the same sampler will be able to collect all the information
    // of a topic.
    String key = ccm.metricClassId() == CruiseControlMetric.MetricClassId.TOPIC_METRIC ?
        ((TopicMetric) ccm).topic() : Integer.toString(ccm.brokerId());
    ProducerRecord<String, CruiseControlMetric> producerRecord =
        new ProducerRecord<>(_cruiseControlMetricsTopic, null, ccm.time(), key, ccm);
    LOG.debug("Sending cruise control metric {}.", ccm);
    _producer.send(producerRecord, new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
          LOG.warn("Failed to send cruise control metric {}", ccm);
          _numMetricSendFailure++;
        }
      }
    });
  }

  private void reportYammerMetrics(long now) throws Exception {
    LOG.debug("Reporting yammer metrics.");
    YammerMetricProcessor.Context context = new YammerMetricProcessor.Context(this, now, _brokerId, _reportingIntervalMs);
    for (Map.Entry<com.yammer.metrics.core.MetricName, Metric> entry : Metrics.defaultRegistry().allMetrics().entrySet()) {
      LOG.trace("Processing yammer metric {}, scope = {}", entry.getKey(), entry.getKey().getScope());
      entry.getValue().processWith(_yammerMetricProcessor, entry.getKey(), context);
    }
    LOG.debug("Finished reporting yammer metrics.");
  }

  private void reportKafkaMetrics(long now) {
    LOG.debug("Reporting KafkaMetrics.");
    for (KafkaMetric metric : _interestedMetrics.values()) {
      sendCruiseControlMetric(MetricsUtils.toCruiseControlMetric(metric, now, _brokerId));
    }
    LOG.debug("Finished reporting KafkaMetrics.");
  }

  private void reportCpuUtils(long now) {
    LOG.debug("Reporting CPU util.");
    sendCruiseControlMetric(MetricsUtils.getCpuMetric(now, _brokerId));
    LOG.debug("Finished reporting CPU util.");
  }

  private void addMetricIfInterested(KafkaMetric metric) {
    LOG.trace("Checking Kafka metric {}", metric.metricName());
    if (MetricsUtils.isInterested(metric.metricName())) {
      LOG.debug("Added new metric {} to cruise control metrics reporter.", metric.metricName());
      _interestedMetrics.put(metric.metricName(), metric);
    }
  }

  private void setIfAbsent(Properties props, String key, String value) {
    if (!props.containsKey(key)) {
      props.setProperty(key, value);
    }
  }

}
