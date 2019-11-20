/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.KafkaTopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.DEFAULT_ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.DEFAULT_BROKER_FAILURE_EXCLUDE_RECENT_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.DEFAULT_GOAL_VIOLATION_EXCLUDE_RECENT_BROKERS_CONFIG;


/**
 * A test util class.
 */
public class KafkaCruiseControlUnitTestUtils {

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
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2121");
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "aaa");
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(KafkaTopicConfigProvider.CLUSTER_CONFIGS_FILE, clusterConfigsFile);
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(KafkaCruiseControlConfig.COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(6)));
    props.setProperty(KafkaCruiseControlConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(24)));
    props.setProperty(KafkaCruiseControlConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(12)));
    props.setProperty(KafkaCruiseControlConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG, "2.0");
    props.setProperty(KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                      Boolean.toString(DEFAULT_ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG));
    props.setProperty(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                      Boolean.toString(DEFAULT_BROKER_FAILURE_EXCLUDE_RECENT_BROKERS_CONFIG));
    props.setProperty(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                      Boolean.toString(DEFAULT_BROKER_FAILURE_EXCLUDE_RECENT_BROKERS_CONFIG));
    props.setProperty(KafkaCruiseControlConfig.GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                      Boolean.toString(DEFAULT_GOAL_VIOLATION_EXCLUDE_RECENT_BROKERS_CONFIG));
    props.setProperty(KafkaCruiseControlConfig.GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                      Boolean.toString(DEFAULT_GOAL_VIOLATION_EXCLUDE_RECENT_BROKERS_CONFIG));
    props.setProperty(KafkaCruiseControlConfig.SELF_HEALING_GOALS_CONFIG, "");
    props.setProperty(
        KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG,
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
