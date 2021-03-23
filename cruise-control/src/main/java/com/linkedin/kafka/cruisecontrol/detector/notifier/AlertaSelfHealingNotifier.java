/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent;
import com.linkedin.kafka.cruisecontrol.detector.TopicAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.TopicPartitionSizeAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomaly.TopicReplicationFactorAnomalyEntry;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;

public class AlertaSelfHealingNotifier extends SelfHealingNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(AlertaSelfHealingNotifier.class);
    public static final String ALERTA_SELF_HEALING_NOTIFIER_API_URL = "alerta.self.healing.notifier.api_url";
    public static final String ALERTA_SELF_HEALING_NOTIFIER_API_KEY = "alerta.self.healing.notifier.api_key";
    public static final String ALERTA_SELF_HEALING_NOTIFIER_ENVIRONMENT = "alerta.self.healing.notifier.environment";
    
    protected String _alertaApiUrl;
    protected String _alertaApiKey;
    protected String _alertaEnvironment;

    public AlertaSelfHealingNotifier() {
    }

    public AlertaSelfHealingNotifier(Time time) {
        super(time);
    }

    @Override
    public void configure(Map<String, ?> config) {
        super.configure(config);
        _alertaApiUrl = (String) config.get(ALERTA_SELF_HEALING_NOTIFIER_API_URL);
        _alertaApiKey = (String) config.get(ALERTA_SELF_HEALING_NOTIFIER_API_KEY);
        _alertaEnvironment = (String) config.get(ALERTA_SELF_HEALING_NOTIFIER_ENVIRONMENT);
    }

    @Override
    public void alert(Anomaly anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
        super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);

        if (_alertaApiUrl == null) {
            LOG.warn("Alerta.io API URL is null, can't send Alerta.io self healing notification");
            return;
        }

        if (_alertaApiKey == null) {
            LOG.warn("Alerta.io API key is null, can't send Alerta.io self healing notification");
            return;
        }

        String text = String.format("%s detected %s. Self healing %s.%s", anomalyType, anomaly,
                _selfHealingEnabled.get(anomalyType) ? String.format("start time %s", utcDateFor(selfHealingStartTime))
                        : "is disabled",
                autoFixTriggered ? "%nSelf-healing has been triggered." : "");

        String tmpLocalHostname;
        try {
          tmpLocalHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
          LOG.warn("Unable to get the hostname of the Cruise Control server");
          tmpLocalHostname = "Cruise Control";
        }
        final String localHostname = tmpLocalHostname;

        DateTimeFormatter outFormatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX") // millisecond precision
            .withZone(ZoneId.of("UTC"));

        List<AlertaMessage> alertaMessages = new ArrayList<>();

        switch ((KafkaAnomalyType) anomalyType) {
          case GOAL_VIOLATION:
            GoalViolations goalViolations = (GoalViolations) anomaly;
            Map<Boolean, List<String>> violations = goalViolations.violatedGoalsByFixability();
            for (Entry<Boolean, List<String>> entry : violations.entrySet()) {
              entry.getValue().forEach(goal -> { 
                AlertaMessage alertaMessage = new AlertaMessage(localHostname, anomalyType.toString() + " - " + goal);
                alertaMessage.setSeverity(severityFromPriority(anomalyType.priority()));
                alertaMessage.setGroup("Performance");
                alertaMessage.setCreateTime(outFormatter.format(Instant.ofEpochMilli(goalViolations.detectionTimeMs())));
                alertaMessages.add(alertaMessage);
              });
            }
            break;
          case BROKER_FAILURE:
            BrokerFailures brokerFailures = (BrokerFailures) anomaly;
            Map<Integer, Long> failedBrokers = brokerFailures.failedBrokers();
            for (Entry<Integer, Long> entry : failedBrokers.entrySet()) {
              AlertaMessage alertaMessage = new AlertaMessage(localHostname + " - Broker " + entry.getKey(),
                  anomalyType.toString());
              alertaMessage.setSeverity(severityFromPriority(anomalyType.priority()));
              alertaMessage.setGroup("Performance");
              alertaMessage.setCreateTime(outFormatter.format(Instant.ofEpochMilli(entry.getValue())));
              alertaMessages.add(alertaMessage);
            }
            break;
          case METRIC_ANOMALY:
            KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) anomaly;
            Map<BrokerEntity, Long> brokersEntitiesValues = metricAnomaly.entities();
            for (Entry<BrokerEntity, Long> entry : brokersEntitiesValues.entrySet()) {
              AlertaMessage alertaMessage = new AlertaMessage(localHostname + " - Broker " + entry.getKey().brokerId(),
                  anomalyType.toString());
              alertaMessage.setSeverity(severityFromPriority(anomalyType.priority()));
              alertaMessage.setGroup("Performance");
              alertaMessage.setCreateTime(outFormatter.format(Instant.ofEpochMilli(entry.getValue())));
              alertaMessages.add(alertaMessage);
            }
            break;
          case DISK_FAILURE:
            DiskFailures diskFailures = (DiskFailures) anomaly;
            Map<Integer, Map<String, Long>> failedDisks = diskFailures.failedDisks();
            failedDisks.forEach((brokerId, failures) -> {
              failures.forEach((logdir, eventTime) -> {
                AlertaMessage alertaMessage = new AlertaMessage(localHostname + " - Broker " + brokerId,
                    anomalyType.toString() + " - " + logdir);
                alertaMessage.setSeverity(severityFromPriority(anomalyType.priority()));
                alertaMessage.setGroup("Storage");
                alertaMessage.setCreateTime(outFormatter.format(Instant.ofEpochMilli(eventTime)));
                alertaMessage.setValue(logdir);
                alertaMessages.add(alertaMessage);
              });
            });
            break;
          case TOPIC_ANOMALY:
            TopicAnomaly topicAnomaly = (TopicAnomaly) anomaly;
            if (topicAnomaly instanceof TopicPartitionSizeAnomaly) {
              TopicPartitionSizeAnomaly topicPartitionSizeAnomaly = (TopicPartitionSizeAnomaly) topicAnomaly;
              for (Map.Entry<TopicPartition, Double> entry : topicPartitionSizeAnomaly.getSizeByPartition().entrySet()) {
                AlertaMessage alertaMessage = new AlertaMessage(localHostname,
                    "TOPIC_PARTITION_SIZE_ANOMALY - " + entry.getKey().toString());
                alertaMessage.setSeverity(severityFromPriority(anomalyType.priority()));
                alertaMessage.setGroup("Performance");
                alertaMessage.setValue(String.format("%f bytes", entry.getValue()));
                alertaMessage.setCreateTime(outFormatter.format(Instant.ofEpochMilli(topicAnomaly.detectionTimeMs())));
                alertaMessages.add(alertaMessage);
              }
            } else if (topicAnomaly instanceof TopicReplicationFactorAnomaly) {
              TopicReplicationFactorAnomaly topicReplicationFactorAnomaly = (TopicReplicationFactorAnomaly) topicAnomaly;
              for (Entry<Short, Set<TopicReplicationFactorAnomalyEntry>> entry : 
                topicReplicationFactorAnomaly.getBadTopicsByReplicationFactor().entrySet()) {
                entry.getValue().forEach(topicReplicationFactorAnomalyEntry -> {
                  AlertaMessage alertaMessage = new AlertaMessage(localHostname,
                      "TOPIC_REPLICATION_FACTOR_ANOMALY - " + topicReplicationFactorAnomalyEntry.topicName());
                  alertaMessage.setSeverity(severityFromPriority(anomalyType.priority()));
                  alertaMessage.setGroup("Performance");
                  alertaMessage.setValue(String.format("%.2f", topicReplicationFactorAnomalyEntry.violationRatio()));
                  alertaMessage.setCreateTime(outFormatter.format(Instant.ofEpochMilli(topicAnomaly.detectionTimeMs())));
                  alertaMessages.add(alertaMessage);
                });
              }
            } else {
              AlertaMessage alertaMessage = new AlertaMessage(localHostname,
                  anomalyType.toString());
              alertaMessage.setSeverity(severityFromPriority(anomalyType.priority()));
              alertaMessage.setGroup("Performance");
              alertaMessage.setCreateTime(outFormatter.format(Instant.ofEpochMilli(topicAnomaly.detectionTimeMs())));
              alertaMessages.add(alertaMessage);
            }
            break;
          case MAINTENANCE_EVENT:
            MaintenanceEvent maintenanceEvent = (MaintenanceEvent) anomaly;
            AlertaMessage alertaMessage = new AlertaMessage(localHostname, anomalyType.toString() + " - " + maintenanceEvent.reasonSupplier());
            alertaMessage.setSeverity(severityFromPriority(anomalyType.priority()));
            alertaMessage.setGroup("Performance");
            alertaMessage.setCreateTime(outFormatter.format(Instant.ofEpochMilli(maintenanceEvent.detectionTimeMs())));
            alertaMessages.add(alertaMessage);
            break;
          default:
            throw new IllegalStateException("Unrecognized anomaly type.");
        }

        for (AlertaMessage alertaMessage : alertaMessages) {
          alertaMessage.setEnvironment(_alertaEnvironment);
          alertaMessage.setService(Arrays.asList("cruise-control"));
          alertaMessage.setText(text);
          alertaMessage.setOrigin("cruise-control/" + localHostname);
          alertaMessage.setType("cruiseControlAlarm");
          alertaMessage.setRawData(anomaly.toString());
          alertaMessage.setTags(Arrays.asList("alarm_id:" + anomaly.anomalyId()));
          try {
              sendAlertaMessage(alertaMessage);
          } catch (IOException e) {
              LOG.warn("ERROR sending alert to Alerta.io", e);
          }
        }
    }

    protected void sendAlertaMessage(AlertaMessage alertaMessage) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(_alertaApiUrl + "/alert");
        StringEntity entity = new StringEntity(alertaMessage.toString());
        httpPost.setEntity(entity);
        httpPost.setHeader("Authorization", "Key " + _alertaApiKey);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        try {
            LOG.debug("Sending alert to Alerta.io : {}\nBody:\n{}", httpPost, alertaMessage);
            CloseableHttpResponse httpResponse = client.execute(httpPost);
            LOG.debug("Alerta.io response status: {}", httpResponse.getStatusLine().getStatusCode());
        } finally {
            client.close();
        }
    }
    
    protected String severityFromPriority(int priority) {
      switch (priority) {
        case 0:
          return "critical";
        case 1:
        case 2:
          return "major";
        case 3:
        case 4:
          return "minor";
        default:
          return "warning";
      }
    }
}
