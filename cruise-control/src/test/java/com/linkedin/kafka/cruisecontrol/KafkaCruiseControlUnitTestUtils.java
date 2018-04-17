/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;


/**
 * A test util class.
 */
public class KafkaCruiseControlUnitTestUtils {

  private KafkaCruiseControlUnitTestUtils() {

  }

  public static Properties getKafkaCruiseControlProperties() {
    Properties props = new Properties();
    String capacityConfigFile =
        KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource("DefaultCapacityConfig.json").getFile();
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2121");
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "aaa");
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG, "2");
    return props;
  }

  public static ZkUtils zkUtils(String zkConnect) {
    ZkConnection zkConnection = new ZkConnection(zkConnect, 30000);
    ZkClient zkClient = new ZkClient(zkConnection, 30000, new ZKStringSerializer());
    return new ZkUtils(zkClient, zkConnection, false);
  }

  private static class ZKStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
  }

  /**
   * Get the aggregated metric values with the given resource usage.
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
    for (int id : KafkaMetricDef.resourceToMetricIds(resource)) {
      MetricValues metricValues = new MetricValues(1);
      if (!set) {
        metricValues.set(0, value);
        set = true;
      }
      aggregatedMetricValues.add(id, metricValues);
    }
  }
}
