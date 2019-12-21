/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaTopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.UserTaskManagerConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * A test util class.
 */
public class KafkaCruiseControlUnitTestUtils {
  public static final int ANOMALY_DETECTOR_INITIAL_QUEUE_SIZE = 10;
  public static final String OPENAPI_SPEC_PATH = System.getProperty("user.dir") + "/src/yaml/base.yaml";
  public static final String CRUISE_CONTROL_PACKAGE = "com.linkedin.kafka.cruisecontrol";
  public static final String JSON_CONTENT_TYPE = "application/json";
  public static final String PLAIN_TEXT_CONTENT_TYPE = "text/plain";

  private KafkaCruiseControlUnitTestUtils() {

  }

  /**
   * @return Kafka Cruise Control properties.
   */
  public static Properties getKafkaCruiseControlProperties() {
    Properties props = new Properties();
    String capacityConfigFile =
        KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource("DefaultCapacityConfig.json").getFile();
    String clusterConfigsFile =
        KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource("DefaultClusterConfigs.json").getFile();
    props.setProperty(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2121");
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "aaa");
    props.setProperty(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(KafkaTopicConfigProvider.CLUSTER_CONFIGS_FILE, clusterConfigsFile);
    props.setProperty(MonitorConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(MonitorConfig.MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(UserTaskManagerConfig.COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(6)));
    props.setProperty(ExecutorConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(24)));
    props.setProperty(ExecutorConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(12)));
    props.setProperty(AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG, "2.0");
    props.setProperty(AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                      Boolean.toString(AnomalyDetectorConfig.DEFAULT_ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG));
    props.setProperty(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                      Boolean.toString(AnomalyDetectorConfig.DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG));
    props.setProperty(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                      Boolean.toString(AnomalyDetectorConfig.DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG));
    props.setProperty(AnomalyDetectorConfig.SELF_HEALING_GOALS_CONFIG, "");
    props.setProperty(
        AnalyzerConfig.DEFAULT_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal");

    return props;
  }

  /**
   * @return The aggregated metric values with the given resource usage.
   */
  public static AggregatedMetricValues getAggregatedMetricValues(double cpuUsage,
                                                                 double networkInBoundUsage,
                                                                 double networkOutBoundUsage,
                                                                 double diskUsage) {
    AggregatedMetricValues aggregateMetricValues = new AggregatedMetricValues();
    setValueForResource(aggregateMetricValues, Resource.CPU, cpuUsage);
    setValueForResource(aggregateMetricValues, Resource.NW_IN, networkInBoundUsage);
    setValueForResource(aggregateMetricValues, Resource.NW_OUT, networkOutBoundUsage);
    setValueForResource(aggregateMetricValues, Resource.DISK, diskUsage);
    return aggregateMetricValues;
  }

  /**
   * Set the utilization values of all metrics for a resource in the given AggregatedMetricValues.
   * The first metric has the full resource utilization value, all the rest of the metrics has 0.
   */
  public static void setValueForResource(AggregatedMetricValues aggregatedMetricValues,
                                         Resource resource,
                                         double value) {
    boolean set = false;
    for (short id : KafkaMetricDef.resourceToMetricIds(resource)) {
      MetricValues metricValues = new MetricValues(1);
      if (!set) {
        metricValues.set(0, value);
        set = true;
      }
      aggregatedMetricValues.add(id, metricValues);
    }
  }
}
