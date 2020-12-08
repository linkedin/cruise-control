/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.RECONNECT_BACKOFF_MS_CONFIG;


/**
 * A util class for Anomaly Detectors.
 */
public class AnomalyDetectorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectorUtils.class);
  public static final String KAFKA_CRUISE_CONTROL_OBJECT_CONFIG = "kafka.cruise.control.object";
  public static final String ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG = "anomaly.detection.time.ms.object";
  public static final long MAX_METADATA_WAIT_MS = TimeUnit.MINUTES.toMillis(1);
  public static final Anomaly SHUTDOWN_ANOMALY = new BrokerFailures();
  public static final Random RANDOM = new Random();

  private AnomalyDetectorUtils() {
  }

  /**
   * Create a Kafka consumer for retrieving reported Maintenance plans.
   * The consumer uses {@link String} for keys and {@link MaintenancePlan} for values.
   *
   * This consumer is not intended to use (1) the group management functionality by using subscribe(topic) or (2) the Kafka-based
   * offset management strategy. Hence, the {@link ConsumerConfig#GROUP_ID_CONFIG} config is irrelevant to it.
   *
   * @param configs The configurations for Cruise Control.
   * @param clientIdPrefix Client id prefix.
   * @return A new Kafka consumer
   */
  @SuppressWarnings("unchecked")
  public static Consumer<String, MaintenancePlan> createMaintenanceEventConsumer(Map<String, ?> configs, String clientIdPrefix) {
    // Get bootstrap servers
    String bootstrapServers = String.join(",", (List<String>) configs.get(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG));

    // Create consumer
    long randomToken = RANDOM.nextLong();
    Properties consumerProps = new Properties();
    consumerProps.putAll(configs);
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-consumer-" + randomToken);
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MaintenancePlanSerde.class.getName());
    consumerProps.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, configs.get(RECONNECT_BACKOFF_MS_CONFIG).toString());
    return new KafkaConsumer<>(consumerProps);
  }

  /**
   * @return A list of names for goals {@link AnomalyDetectorConfig#SELF_HEALING_GOALS_CONFIG} in the order of priority.
   */
  public static List<String> getSelfHealingGoalNames(KafkaCruiseControlConfig config) {
    List<Goal> goals = config.getConfiguredInstances(AnomalyDetectorConfig.SELF_HEALING_GOALS_CONFIG, Goal.class);
    List<String> selfHealingGoalNames = new ArrayList<>(goals.size());
    for (Goal goal : goals) {
      selfHealingGoalNames.add(goal.name());
    }
    return selfHealingGoalNames;
  }

  /**
   * Retrieve the {@link AnomalyDetectionStatus anomaly detection status}, indicating whether an anomaly detector is
   * ready to check for an anomaly.
   *
   * <li>See {@link AnomalyDetectionStatus} for details.</li>
   *
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param checkOfflineReplica {@code true} to skip anomaly detection if there are offline replicas, {@code false} otherwise.
   * @param checkOngoingExecution {@code true} to skip anomaly detection if there is an ongoing execution, {@code false} otherwise.
   * @return The {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the anomaly detector is ready.
   */
  static AnomalyDetectionStatus getAnomalyDetectionStatus(KafkaCruiseControl kafkaCruiseControl,
                                                          boolean checkOfflineReplica,
                                                          boolean checkOngoingExecution) {
    if (checkOfflineReplica) {
      Set<Integer> brokersWithOfflineReplicas = kafkaCruiseControl.loadMonitor().brokersWithOfflineReplicas(MAX_METADATA_WAIT_MS);
      if (!brokersWithOfflineReplicas.isEmpty()) {
        LOG.info("Skipping anomaly detection because there are dead brokers/disks in the cluster, flawed brokers: {}",
                 brokersWithOfflineReplicas);
        return AnomalyDetectionStatus.SKIP_HAS_OFFLINE_REPLICAS;
      }
    }
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState = kafkaCruiseControl.getLoadMonitorTaskRunnerState();
    if (!AnomalyUtils.isLoadMonitorReady(loadMonitorTaskRunnerState)) {
      LOG.info("Skipping anomaly detection because load monitor is in {} state.", loadMonitorTaskRunnerState);
      return AnomalyDetectionStatus.SKIP_LOAD_MONITOR_NOT_READY;
    }

    if (checkOngoingExecution) {
      ExecutorState.State executionState = kafkaCruiseControl.executionState();
      if (executionState != ExecutorState.State.NO_TASK_IN_PROGRESS) {
        LOG.info("Skipping anomaly detection because the executor is in {} state.", executionState);
        return AnomalyDetectionStatus.SKIP_EXECUTOR_NOT_READY;
      }
    }

    return AnomalyDetectionStatus.READY;
  }

  /**
   * Get a comparator of anomaly based on anomaly type and anomaly detection time.
   *
   * @return The anomaly comparator.
   */
  public static Comparator<Anomaly> anomalyComparator() {
    return Comparator.comparing((Anomaly anomaly) -> anomaly.anomalyType().priority())
                     .thenComparingLong(Anomaly::detectionTimeMs);
  }

  /**
   * Check whether the given goal violations has unfixable goals.
   *
   * @param goalViolations Goal violations to check whether there are unfixable goals.
   * @return True if the given goal violations contain unfixable goals, false otherwise.
   */
  public static boolean hasUnfixableGoals(GoalViolations goalViolations) {
    List<String> unfixableGoals = goalViolations.violatedGoalsByFixability().get(false);
    return unfixableGoals != null && !unfixableGoals.isEmpty();
  }
}
