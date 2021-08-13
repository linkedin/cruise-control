/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertThrows;


/**
 * Unit test for testing goals with excluded topics under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class CaseInsensitiveGoalConfigTest {
  private static final Logger LOG = LoggerFactory.getLogger(CaseInsensitiveGoalConfigTest.class);

  private final Properties _properties;
  private final Class<Throwable> _exceptionClass;

  public CaseInsensitiveGoalConfigTest(Properties properties, Class<Throwable> exceptionClass) {
    _properties = properties;
    _exceptionClass = exceptionClass;
  }

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
    caseInsensitiveGoalProps.setProperty(AnalyzerConfig.GOALS_CONFIG, TestConstants.GOALS_VALUES);
    caseInsensitiveGoalProps.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);

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
    caseSensitiveGoalProps.setProperty(
        AnalyzerConfig.GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.RaCkAwArEgOaL");

    Object[] withCaseSensitiveGoalParams = {caseSensitiveGoalProps, ConfigException.class};
    params.add(withCaseSensitiveGoalParams);

    return params;
  }

  @Test
  public void test() {
    LOG.debug("Testing case insensitive goal configuration: {}.", _properties);

    if (_exceptionClass != null) {
      assertThrows(_exceptionClass, () -> new KafkaCruiseControlConfig(_properties));
    } else {
      new KafkaCruiseControlConfig(_properties);
    }
  }
}
