/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing goals with excluded topics under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class CaseInsensitiveGoalConfigTest {
  private static final Logger LOG = LoggerFactory.getLogger(CaseInsensitiveGoalConfigTest.class);

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Parameters
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> params = new ArrayList<>();


    Properties sharedProps = new Properties();
    sharedProps.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2121");
    sharedProps.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "aaa");

    // Test: With case insensitive goal names (No exception)
    Properties caseInsensitiveGoalProps = new Properties();
    caseInsensitiveGoalProps.putAll(sharedProps);
    caseInsensitiveGoalProps.setProperty(KafkaCruiseControlConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal");

    Object[] withCaseInsensitiveGoalParams = {caseInsensitiveGoalProps, null};
    params.add(withCaseInsensitiveGoalParams);

    // Test: With duplicate goal names under different packages (Exception)
    Properties duplicateGoalProps = new Properties();
    duplicateGoalProps.putAll(sharedProps);
    duplicateGoalProps.setProperty(KafkaCruiseControlConfig.GOALS_CONFIG,
        "com.linkedin.package1.RackAwareGoal,com.linkedin.package2.RackAwareGoal");

    Object[] withDuplicateGoalParams = {duplicateGoalProps, ConfigException.class};
    params.add(withDuplicateGoalParams);

    // Test: With case sensitive goal names (Exception)
    Properties caseSensitiveGoalProps = new Properties();
    caseSensitiveGoalProps.putAll(sharedProps);
    caseSensitiveGoalProps.setProperty(KafkaCruiseControlConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.RaCkAwArEgOaL");

    Object[] withCaseSensitiveGoalParams = {caseSensitiveGoalProps, ConfigException.class};
    params.add(withCaseSensitiveGoalParams);

    return params;
  }

  private Properties _properties;
  private Class<Throwable> _exceptionClass;

  public CaseInsensitiveGoalConfigTest(Properties properties, Class<Throwable> exceptionClass) {
    _properties = properties;
    _exceptionClass = exceptionClass;
  }

  @Test
  public void test() throws Exception {
    LOG.debug("Testing case insensitive goal configuration: {}.", _properties.toString());

    if (_exceptionClass != null) {
      expected.expect(_exceptionClass);
    }

    new KafkaCruiseControlConfig(_properties);
    assertTrue("Failed to detect case insensitive goal configs.", true);
  }
}