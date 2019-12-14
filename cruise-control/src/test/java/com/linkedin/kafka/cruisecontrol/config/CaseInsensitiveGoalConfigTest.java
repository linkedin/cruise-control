/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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
  public ExpectedException _expected = ExpectedException.none();

  /**
   * Populate parameters for the parametrized test.
   * @return Populated parameters.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<>();


    Properties sharedProps = new Properties();
    sharedProps.setProperty(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2121");
    sharedProps.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "aaa");

    // Test: With case insensitive goal names (No exception)
    Properties caseInsensitiveGoalProps = new Properties();
    caseInsensitiveGoalProps.putAll(sharedProps);
    caseInsensitiveGoalProps.setProperty(AnalyzerConfig.GOALS_CONFIG,
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
    caseInsensitiveGoalProps.setProperty(
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

    Object[] withCaseInsensitiveGoalParams = {caseInsensitiveGoalProps, null};
    params.add(withCaseInsensitiveGoalParams);

    // Test: With duplicate goal names under different packages (Exception)
    Properties duplicateGoalProps = new Properties();
    duplicateGoalProps.putAll(sharedProps);
    duplicateGoalProps.setProperty(AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.package1.RackAwareGoal,com.linkedin.package2.RackAwareGoal");

    Object[] withDuplicateGoalParams = {duplicateGoalProps, ConfigException.class};
    params.add(withDuplicateGoalParams);

    // Test: With case sensitive goal names (Exception)
    Properties caseSensitiveGoalProps = new Properties();
    caseSensitiveGoalProps.putAll(sharedProps);
    caseSensitiveGoalProps.setProperty(AnalyzerConfig.GOALS_CONFIG,
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
  public void test() {
    LOG.debug("Testing case insensitive goal configuration: {}.", _properties);

    if (_exceptionClass != null) {
      _expected.expect(_exceptionClass);
    }

    new KafkaCruiseControlConfig(_properties);
    assertTrue("Failed to detect case insensitive goal configs.", true);
  }
}