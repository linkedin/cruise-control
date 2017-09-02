/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;

import com.linkedin.kafka.cruisecontrol.monitor.sampling.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import java.util.Properties;
import org.apache.kafka.common.TopicPartition;


/**
 * A test util class.
 */
public class CruiseControlUnitTestUtils {

  private CruiseControlUnitTestUtils() {

  }

  public static Properties getCruiseControlProperties() {
    Properties props = new Properties();
    String capacityConfigFile =
        CruiseControlUnitTestUtils.class.getClassLoader().getResource("DefaultCapacityConfig.json").getFile();
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2121");
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "aaa");
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG, "2");
    return props;
  }

  public static void populateSampleAggregator(int numSnapshots, 
                                              int numSamplesPerSnapshot, 
                                              MetricSampleAggregator metricSampleAggregator, 
                                              TopicPartition tp, 
                                              int startingSnapshotWindow, 
                                              long snapshotWindowMs) {
    for (int i = startingSnapshotWindow; i < numSnapshots + startingSnapshotWindow; i++) {
      for (int j = 0; j < numSamplesPerSnapshot; j++) {
        PartitionMetricSample sample = new PartitionMetricSample(0, tp);
        sample.record(Resource.DISK, i * 10 + j);
        sample.record(Resource.CPU, i * 10 + j);
        sample.record(Resource.NW_IN, i * 10 + j);
        sample.record(Resource.NW_OUT, i * 10 + j);
        sample.close(i * snapshotWindowMs + 1);
        metricSampleAggregator.addSample(sample);
      }
    }
  }
}
