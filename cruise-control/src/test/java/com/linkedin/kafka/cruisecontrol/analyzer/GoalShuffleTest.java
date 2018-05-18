/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.SortedMap;
import java.util.StringJoiner;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class GoalShuffleTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{0, 2}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 6}});
  }

  private int _numPrecomuptingThreadCfg;
  private int _numPrecomuptingThreadExp;


  public GoalShuffleTest(int threadNumCfg, int threadNumExp) {
    this._numPrecomuptingThreadCfg = threadNumCfg;
    this._numPrecomuptingThreadExp = threadNumExp;
  }

  @Test
  public void testGoalGetShuffled() throws Exception {

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG, Long.toString(_numPrecomuptingThreadCfg));
    props.setProperty(KafkaCruiseControlConfig.GOALS_CONFIG, new StringJoiner(",")
        .add(RackAwareGoal.class.getName())
        .add(ReplicaCapacityGoal.class.getName())
        .add(DiskCapacityGoal.class.getName()).toString());
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    GoalOptimizer goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(balancingConstraint.setProps(props)),
        null,
        new SystemTime(),
        new MetricRegistry());
    List<SortedMap<Integer, Goal>> randomizedGoal = goalOptimizer.getgoalByPriorityForPrecomputing();

    //Check correct number of goal priority is generated
    assertEquals(_numPrecomuptingThreadExp, randomizedGoal.size());

    //Check the first generated goal priority has the same order as set in config
    assertTrue(randomizedGoal.get(0).get(0) instanceof RackAwareGoal);
    assertTrue(randomizedGoal.get(0).get(1) instanceof ReplicaCapacityGoal);
    assertTrue(randomizedGoal.get(0).get(2) instanceof DiskCapacityGoal);

    //Check all generated goals priorities are valid
    for (int i = 0; i < randomizedGoal.size(); i++) {
      for (int j = 0; j < 3; j++) {
        assertTrue(randomizedGoal.get(i).keySet().contains(j));
      }
    }

    //Check all generated goal priorities are unique
    for (int i = 0; i < randomizedGoal.size() - 1; i++) {
      for (int j = i + 1; j < randomizedGoal.size(); j++) {
        assertTrue(!randomizedGoal.get(i).toString().equals(randomizedGoal.get(j).toString()));
      }
    }
  }
}
