/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Properties;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Test;


public class GoalOptimizerTest {

  @Test
  public void testNoPreComputingThread() {
    Properties props = new Properties();
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234");
    props.setProperty(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG, "0");
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
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, EasyMock.mock(LoadMonitor.class), new SystemTime(),
                                                    new MetricRegistry());
    // Should exit immediately.
    goalOptimizer.run();
  }
}
