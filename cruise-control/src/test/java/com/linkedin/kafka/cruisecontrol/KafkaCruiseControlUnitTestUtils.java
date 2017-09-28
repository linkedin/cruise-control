/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;

import java.util.Properties;


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
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_LOAD_SNAPSHOT_CONFIG, "2");
    return props;
  }
}
