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
    return Arrays.asList(new Object[][]{{0, 1, false}, {1, 1, false}, {3, 3, false},  {6, 6, false}, {7, 6, false},
                                        {0, 1, true}, {1, 1, true},  {16, 16, true}, {32, 32, true}});
  }

  private int _numPrecomputingThreadConfig;
  private int _numPrecomputingThreadExpect;
  private boolean _emptyGoalConfig;


  public GoalShuffleTest(int numPrecomputingThreadConfig, int numPrecomputingThreadExpect, boolean emptyGoalConfig) {
    _numPrecomputingThreadConfig = numPrecomputingThreadConfig;
    _numPrecomputingThreadExpect = numPrecomputingThreadExpect;
    _emptyGoalConfig = emptyGoalConfig;
  }

  private void validateOriginalGoalOrder(List<SortedMap<Integer, Goal>> goalByPriorityForPrecomputing) {
    // Check whether one of the generated goal priorities has the same order as set in config.
    boolean foundTheOriginalGoalPriorities = false;
    for (SortedMap<Integer, Goal> goalByPriority : goalByPriorityForPrecomputing) {
      foundTheOriginalGoalPriorities = goalByPriority.get(0).name().equals(RackAwareGoal.class.getSimpleName())
                                       && goalByPriority.get(1).name().equals(ReplicaCapacityGoal.class.getSimpleName())
                                       && goalByPriority.get(2).name().equals(DiskCapacityGoal.class.getSimpleName());

      if (foundTheOriginalGoalPriorities) {
        break;
      }
    }
    assertTrue("The original priorities is missing from the generated priorities.", foundTheOriginalGoalPriorities);
  }

  @Test
  public void testGoalGetShuffled() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG, Long.toString(_numPrecomputingThreadConfig));
    if (_emptyGoalConfig) {
      props.setProperty(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG, "");
    } else {
      props.setProperty(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG, new StringJoiner(",").add(RackAwareGoal.class.getName())
                                                                                            .add(ReplicaCapacityGoal.class.getName())
                                                                                            .add(DiskCapacityGoal.class.getName())
                                                                                            .toString());
    }
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    GoalOptimizer goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(balancingConstraint.setProps(props)),
                                                    null,
                                                    new SystemTime(),
                                                    new MetricRegistry());
    List<SortedMap<Integer, Goal>> goalByPriorityForPrecomputing = goalOptimizer.goalByPriorityForPrecomputing();

    // Check whether the correct number of goal priority is generated
    assertEquals(_numPrecomputingThreadExpect, goalByPriorityForPrecomputing.size());
    validateOriginalGoalOrder(goalByPriorityForPrecomputing);

    // Check whether all generated goal priorities are valid.
    for (SortedMap<Integer, Goal> aGoalByPriorityForPrecomputing : goalByPriorityForPrecomputing) {
      for (int j = 0; j < 3; j++) {
        assertTrue(aGoalByPriorityForPrecomputing.keySet().contains(j));
      }
    }

    // Check all generated goal priorities are unique.
    for (int i = 0; i < goalByPriorityForPrecomputing.size() - 1; i++) {
      for (int j = i + 1; j < goalByPriorityForPrecomputing.size(); j++) {
        assertTrue(!goalByPriorityForPrecomputing.get(i).toString().equals(goalByPriorityForPrecomputing.get(j).toString()));
      }
    }
  }
}
