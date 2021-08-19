/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import java.lang.reflect.Constructor;
import java.util.Properties;

public final class AnalyzerUnitTestUtils {

  private AnalyzerUnitTestUtils() {

  }

  /**
   * Reflectively create goal object from specified goal class and config overrides.
   *
   * @param goalClass The goal class to create object.
   * @param configOverrides Config overrides.
   * @return New object of specified class.
   */
  public static Goal goal(Class<? extends Goal> goalClass, Properties configOverrides) throws Exception {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(5L));
    props.setProperty(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(5L));
    props.setProperty(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, Double.toString(1.2));
    configOverrides.stringPropertyNames().forEach(k -> props.setProperty(k, configOverrides.getProperty(k)));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    try {
      Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
      constructor.setAccessible(true);
      return constructor.newInstance(balancingConstraint);
    } catch (NoSuchMethodException badConstructor) {
      //Try default constructor
      return goalClass.getDeclaredConstructor().newInstance();
    }
  }

  /**
   * Reflectively create goal object from specified goal class.
   *
   * @param goalClass The goal class to create object.
   * @return New object of specified class.
   */
  public static Goal goal(Class<? extends Goal> goalClass) throws Exception {
    return goal(goalClass, new Properties());
  }
}
