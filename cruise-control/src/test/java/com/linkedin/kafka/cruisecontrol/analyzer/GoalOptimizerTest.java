/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Properties;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Test;


public class GoalOptimizerTest {

  @Test
  public void testNoPreComputingThread() {
    Properties props = new Properties();
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
    props.setProperty(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234");
    props.setProperty(AnalyzerConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG, "0");
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, EasyMock.mock(LoadMonitor.class), new SystemTime(),
                                                    new MetricRegistry(), EasyMock.mock(Executor.class));
    // Should exit immediately.
    goalOptimizer.run();
  }
}
