/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.CruiseControlMetricsReporterException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricsUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.YammerMetricProcessor;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.log.LogConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ReassignmentInProgressException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.maybeUpdateConfig;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.CLIENT_REQUEST_TIMEOUT_MS;

public class CruiseControlMetricsReporter implements MetricsReporter, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsReporter.class);
  private YammerMetricProcessor _yammerMetricProcessor;
  private final Map<org.apache.kafka.common.MetricName, KafkaMetric> _interestedMetrics = new ConcurrentHashMap<>();
  private KafkaThread _metricsReporterRunner;
  private KafkaProducer<String, CruiseControlMetric> _producer;
  private String _cruiseControlMetricsTopic;
  private long _reportingIntervalMs;
  private int _brokerId;
  private long _lastReportingTime = System.currentTimeMillis();
  private int _numMetricSendFailure = 0;
  private volatile boolean _shutdown = false;
  private NewTopic _metricsTopic;
  private AdminClient _adminClient;
  private long _metricsTopicAutoCreateTimeoutMs;
  private int _metricsTopicAutoCreateRetries;
  protected static final String CRUISE_CONTROL_METRICS_TOPIC_CLEAN_UP_POLICY = "delete";
  protected static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(5);
  private boolean _kubernetesMode;
  public static final String DEFAULT_BOOTSTRAP_SERVERS_HOST = "localhost";
  public static final String DEFAULT_BOOTSTRAP_SERVERS_PORT = "9092";

  @Override
  public void init(List<KafkaMetric> metrics) {
    for (KafkaMetric kafkaMetric : metrics) {
      addMetricIfInterested(kafkaMetric);
    }
    LOG.info("Added {} Kafka metrics for Cruise Control metrics during initialization.", _interestedMetrics.size());
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
    LOG.info("Closing Cruise Control metrics reporter.");
    _shutdown = true;
    if (_metricsReporterRunner != null) {
      _metricsReporterRunner.interrupt();
    }
    if (_producer != null) {
      _producer.close(PRODUCER_CLOSE_TIMEOUT);
    }
  }

  static String getBootstrapServers(Map<String, ?> configs) {
    Object port = configs.get(KafkaConfig.PortProp());
    String listeners = String.valueOf(configs.get(KafkaConfig.ListenersProp()));
    if (!listeners.equals("null") && listeners.length() != 0) {
      // See https://kafka.apache.org/documentation/#listeners for possible responses. If multiple listeners are configured, this function
      // picks the first listener in the list of listeners. Hence, users of this config must adjust their order accordingly.
      String firstListener = listeners.split("\\s*,\\s*")[0];
      String[] protocolHostPort = firstListener.split(":");
      // Use port of listener only if no explicit config specified for KafkaConfig.PortProp().
      String portToUse = port == null ? protocolHostPort[protocolHostPort.length - 1] : String.valueOf(port);
      // Use host of listener if one is specified.
      return ((protocolHostPort[1].length() == 2) ? DEFAULT_BOOTSTRAP_SERVERS_HOST : protocolHostPort[1].substring(2)) + ":" + portToUse;
    }

    return DEFAULT_BOOTSTRAP_SERVERS_HOST + ":" + (port == null ? DEFAULT_BOOTSTRAP_SERVERS_PORT : port);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Properties producerProps = CruiseControlMetricsReporterConfig.parseProducerConfigs(configs);

    //Add BootstrapServers if not set
    if (!producerProps.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
      String bootstrapServers = getBootstrapServers(configs);
      producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      LOG.info("Using default value of {} for {}", bootstrapServers,
               CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    //Add SecurityProtocol if not set
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
    setIfAbsent(producerProps, ProducerConfig.LINGER_MS_CONFIG,
        reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_CONFIG).toString());
    setIfAbsent(producerProps, ProducerConfig.BATCH_SIZE_CONFIG,
        reporterConfig.getInt(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_CONFIG).toString());
    setIfAbsent(producerProps, ProducerConfig.RETRIES_CONFIG, "5");
    setIfAbsent(producerProps, ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    setIfAbsent(producerProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    setIfAbsent(producerProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
    setIfAbsent(producerProps, ProducerConfig.ACKS_CONFIG, "all");
    _producer = new KafkaProducer<>(producerProps);

    _brokerId = Integer.parseInt((String) configs.get(KafkaConfig.BrokerIdProp()));

    _cruiseControlMetricsTopic = reporterConfig.getString(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG);
    _reportingIntervalMs = reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG);
    _kubernetesMode = reporterConfig.getBoolean(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_KUBERNETES_MODE_CONFIG);

    if (reporterConfig.getBoolean(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG)) {
      try {
        _metricsTopic = createMetricsTopicFromReporterConfig(reporterConfig);
        Properties adminClientConfigs = CruiseControlMetricsUtils.addSslConfigs(producerProps, reporterConfig);
        _adminClient = CruiseControlMetricsUtils.createAdminClient(adminClientConfigs);
        _metricsTopicAutoCreateTimeoutMs = reporterConfig.getLong(
            CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS_CONFIG);
        _metricsTopicAutoCreateRetries = reporterConfig.getInt(
            CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES_CONFIG);
      } catch (CruiseControlMetricsReporterException e) {
        LOG.warn("Cruise Control metrics topic auto creation was disabled", e);
      }
    }
  }

  protected NewTopic createMetricsTopicFromReporterConfig(CruiseControlMetricsReporterConfig reporterConfig)
      throws CruiseControlMetricsReporterException {
    String cruiseControlMetricsTopic =
        reporterConfig.getString(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG);
    Integer cruiseControlMetricsTopicNumPartition =
        reporterConfig.getInt(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG);
    Short cruiseControlMetricsTopicReplicaFactor =
        reporterConfig.getShort(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG);
    Short cruiseControlMetricsTopicMinInsyncReplicas =
            reporterConfig.getShort(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_MIN_INSYNC_REPLICAS_CONFIG);

    if (cruiseControlMetricsTopicReplicaFactor <= 0 || cruiseControlMetricsTopicNumPartition <= 0) {
      throw new CruiseControlMetricsReporterException("The topic configuration must explicitly set the replication factor and the num partitions");
    }

    NewTopic newTopic = new NewTopic(cruiseControlMetricsTopic, cruiseControlMetricsTopicNumPartition, cruiseControlMetricsTopicReplicaFactor);

    Map<String, String> config = new HashMap<>(cruiseControlMetricsTopicMinInsyncReplicas > 0 ? 3 : 2);
    config.put(LogConfig.RetentionMsProp(),
               Long.toString(reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_RETENTION_MS_CONFIG)));
    config.put(LogConfig.CleanupPolicyProp(), CRUISE_CONTROL_METRICS_TOPIC_CLEAN_UP_POLICY);
    if (cruiseControlMetricsTopicMinInsyncReplicas > 0) {
      // If the user has set the minISR for the metrics topic we need to check that the replication factor is set to a level that allows the
      // minISR to be met.
      if (cruiseControlMetricsTopicReplicaFactor >= cruiseControlMetricsTopicMinInsyncReplicas) {
        config.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(cruiseControlMetricsTopicMinInsyncReplicas));
      } else {
        throw new CruiseControlMetricsReporterException(String.format(
                "The configured topic replication factor (%d) must be greater than or equal to" +
                "the configured topic minimum insync replicas (%d)",
                cruiseControlMetricsTopicReplicaFactor,
                cruiseControlMetricsTopicMinInsyncReplicas));
      }
      // If the user does not set the metrics minISR we do not set that config and use the Kafka cluster's default.
    }
    newTopic.configs(config);
    return newTopic;
  }

  protected void createCruiseControlMetricsTopic() throws TopicExistsException {
    CruiseControlMetricsUtils.retry(() -> {
      try {
        CreateTopicsResult createTopicsResult = _adminClient.createTopics(Collections.singletonList(_metricsTopic));
        createTopicsResult.values().get(_metricsTopic.name()).get(_metricsTopicAutoCreateTimeoutMs, TimeUnit.MILLISECONDS);
        LOG.info("Cruise Control metrics topic {} is created.", _metricsTopic.name());
        return false;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        if (e.getCause() instanceof TopicExistsException) {
          throw (TopicExistsException) e.getCause();
        }
        LOG.warn("Unable to create Cruise Control metrics topic {}.", _metricsTopic.name(), e);
        return true;
      }
    }, _metricsTopicAutoCreateRetries);
  }

  protected void maybeUpdateCruiseControlMetricsTopic() {
    maybeUpdateTopicConfig();
    maybeIncreaseTopicPartitionCount();
  }

  protected void maybeUpdateTopicConfig() {
    try {
      // Retrieve topic config to check and update.
      ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, _cruiseControlMetricsTopic);
      DescribeConfigsResult describeConfigsResult = _adminClient.describeConfigs(Collections.singleton(topicResource));
      Config topicConfig = describeConfigsResult.values().get(topicResource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      Set<AlterConfigOp> alterConfigOps = new HashSet<>(2);
      Map<String, String> configsToSet = new HashMap<>(2);
      configsToSet.put(LogConfig.RetentionMsProp(), _metricsTopic.configs().get(LogConfig.RetentionMsProp()));
      configsToSet.put(LogConfig.CleanupPolicyProp(), _metricsTopic.configs().get(LogConfig.CleanupPolicyProp()));
      maybeUpdateConfig(alterConfigOps, configsToSet, topicConfig);
      if (!alterConfigOps.isEmpty()) {
        AlterConfigsResult alterConfigsResult = _adminClient.incrementalAlterConfigs(Collections.singletonMap(topicResource, alterConfigOps));
        alterConfigsResult.values().get(topicResource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.warn("Unable to update config of Cruise Cruise Control metrics topic {}", _cruiseControlMetricsTopic, e);
    }
  }

  protected void maybeIncreaseTopicPartitionCount() {
    String cruiseControlMetricsTopic = _metricsTopic.name();
    try {
      // Retrieve topic partition count to check and update.
      TopicDescription topicDescription =
          _adminClient.describeTopics(Collections.singletonList(cruiseControlMetricsTopic)).values()
                      .get(cruiseControlMetricsTopic).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (topicDescription.partitions().size() < _metricsTopic.numPartitions()) {
        _adminClient.createPartitions(Collections.singletonMap(cruiseControlMetricsTopic,
                                                               NewPartitions.increaseTo(_metricsTopic.numPartitions())));
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.warn("Partition count increase to {} for topic {} failed{}.", _metricsTopic.numPartitions(), cruiseControlMetricsTopic,
               (e.getCause() instanceof ReassignmentInProgressException) ? " due to ongoing reassignment" : "", e);
    }
  }

  @Override
  public void run() {
    LOG.info("Starting Cruise Control metrics reporter with reporting interval of {} ms.", _reportingIntervalMs);
    if (_metricsTopic != null && _adminClient != null) {
      try {
        createCruiseControlMetricsTopic();
      } catch (TopicExistsException e) {
        maybeUpdateCruiseControlMetricsTopic();
      } finally {
        CruiseControlMetricsUtils.closeAdminClientWithTimeout(_adminClient);
      }
    }

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
              LOG.info("Cruise Control metric reporter is interrupted during flush due to shutdown request.");
            } else {
              throw ie;
            }
          }
        } catch (Exception e) {
          LOG.error("Got exception in Cruise Control metrics reporter", e);
        }
        // Log failures if there is any.
        if (_numMetricSendFailure > 0) {
          LOG.warn("Failed to send {} metrics for time {}", _numMetricSendFailure, now);
        }
        _numMetricSendFailure = 0;
        long nextReportTime = now + _reportingIntervalMs;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reporting finished for time {} in {} ms. Next reporting time {}",
                    now, System.currentTimeMillis() - now, nextReportTime);
        }
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
      LOG.info("Cruise Control metrics reporter exited.");
    }
  }

  /**
   * Send a CruiseControlMetric to the Kafka topic.
   * @param ccm the Cruise Control metric to send.
   */
  public void sendCruiseControlMetric(CruiseControlMetric ccm) {
    // Use topic name as key if existing so that the same sampler will be able to collect all the information
    // of a topic.
    String key = ccm.metricClassId() == CruiseControlMetric.MetricClassId.TOPIC_METRIC ?
        ((TopicMetric) ccm).topic() : Integer.toString(ccm.brokerId());
    ProducerRecord<String, CruiseControlMetric> producerRecord =
        new ProducerRecord<>(_cruiseControlMetricsTopic, null, ccm.time(), key, ccm);
    LOG.debug("Sending Cruise Control metric {}.", ccm);
    _producer.send(producerRecord, new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
          LOG.warn("Failed to send Cruise Control metric {}", ccm);
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
    LOG.debug("Reporting KafkaMetrics. {}", _interestedMetrics.values());
    for (KafkaMetric metric : _interestedMetrics.values()) {
      sendCruiseControlMetric(MetricsUtils.toCruiseControlMetric(metric, now, _brokerId));
    }
    LOG.debug("Finished reporting KafkaMetrics.");
  }

  private void reportCpuUtils(long now) throws IOException {
    LOG.debug("Reporting CPU util.");
    sendCruiseControlMetric(MetricsUtils.getCpuMetric(now, _brokerId, _kubernetesMode));
    LOG.debug("Finished reporting CPU util.");
  }

  private void addMetricIfInterested(KafkaMetric metric) {
    LOG.trace("Checking Kafka metric {}", metric.metricName());
    if (MetricsUtils.isInterested(metric.metricName())) {
      LOG.debug("Added new metric {} to Cruise Control metrics reporter.", metric.metricName());
      _interestedMetrics.put(metric.metricName(), metric);
    }
  }

  private void setIfAbsent(Properties props, String key, String value) {
    if (!props.containsKey(key)) {
      props.setProperty(key, value);
    }
  }

}
