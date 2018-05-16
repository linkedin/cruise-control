/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import java.util.SortedMap;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.Test;


import static org.junit.Assert.*;


public class GoalShuffleTest {

//  private static final Logger LOG = LoggerFactory.getLogger(GoalShuffleTest.class);

  @Test
  public void testGoalGetShuffled() {

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG, Long.toString(5));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);


    System.out.println("property applied is " + props);
    GoalOptimizer goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(balancingConstraint.setProps(props)),
        null,
        new SystemTime(),
        new MetricRegistry());
    try {
      Field field = goalOptimizer.getClass().getDeclaredField("_goalByPriorityForPrecomputing");
      field.setAccessible(true);
      List<SortedMap<Integer, Goal>> randomizedGoal = (List<SortedMap<Integer, Goal>>) field.get(goalOptimizer);
      System.out.println("generated goals are" + randomizedGoal);
      for (int i = 0; i < randomizedGoal.size() - 1; i++) {
        for (int j = i + 1; j < randomizedGoal.size(); j++) {
          assertNotEquals(randomizedGoal.get(i), randomizedGoal.get(j));
        }
      assertEquals(randomizedGoal.size(), 5);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
