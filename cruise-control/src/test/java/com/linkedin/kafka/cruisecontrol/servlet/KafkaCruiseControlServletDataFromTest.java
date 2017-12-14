/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerState;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitorState;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class KafkaCruiseControlServletDataFromTest {
  private static final ModelCompletenessRequirements FOR_AVAILABLE_WINDOWS =
      new ModelCompletenessRequirements(1, 1.0, true);
  private static final ModelCompletenessRequirements FOR_AVAILABLE_PARTITIONS =
      new ModelCompletenessRequirements(Integer.MAX_VALUE, 0.0, true);
  
  private final int _numReadyGoals;
  private final int _totalGoals;
  private final int _numValidWindows;
  private final KafkaCruiseControlServlet.DataFrom _dataFrom;
  private final List<String> _expectedGoalsToUse;
  private final ModelCompletenessRequirements _expectedRequirements;
  
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    // all goals are ready, 1 valid window, with available windows.
    params.add(new Object[]{3, 3, 1, KafkaCruiseControlServlet.DataFrom.VALID_WINDOWS, Collections.emptyList(), null});
    // 2 out of 3 goals are ready, 1 valid window, with available windows.
    params.add(new Object[]{2, 3, 1, KafkaCruiseControlServlet.DataFrom.VALID_WINDOWS, Collections.emptyList(), FOR_AVAILABLE_WINDOWS});
    // all goals are ready, 1 valid window, with available partitions.
    params.add(new Object[]{3, 3, 1, KafkaCruiseControlServlet.DataFrom.VALID_PARTITIONS, Collections.emptyList(), FOR_AVAILABLE_PARTITIONS});
    // 2 out of 3 goals are ready, 1 valid window, with available partitions.
    params.add(new Object[]{2, 3, 1, KafkaCruiseControlServlet.DataFrom.VALID_PARTITIONS, Collections.emptyList(), FOR_AVAILABLE_PARTITIONS});
    // 2 out of 3 goals are ready, 0 valid window, with available windows. 
    params.add(new Object[]{2, 3, 0, KafkaCruiseControlServlet.DataFrom.VALID_WINDOWS, Arrays.asList("0", "1"), null});
    // No goal is ready, 0 valid window, with available windows. 
    params.add(new Object[]{0, 3, 0, KafkaCruiseControlServlet.DataFrom.VALID_WINDOWS, Collections.emptyList(), null});
    return params;
  }
  
  public KafkaCruiseControlServletDataFromTest(int numReadyGoals, int totalGoals, int numValidWindows,
                                               KafkaCruiseControlServlet.DataFrom dataFrom,
                                               List<String> expectedGoalsToUse,
                                               ModelCompletenessRequirements expectedRequirements) {
    _numReadyGoals = numReadyGoals;
    _totalGoals = totalGoals;
    _numValidWindows = numValidWindows;
    _dataFrom = dataFrom;
    _expectedGoalsToUse = expectedGoalsToUse;
    _expectedRequirements = expectedRequirements;
  }
  
  @Test
  public void test() {
    KafkaCruiseControl mockKCC = EasyMock.createMock(KafkaCruiseControl.class);
    KafkaCruiseControlState kccState = getState(_numReadyGoals, _totalGoals, _numValidWindows);
    EasyMock.expect(mockKCC.state()).andReturn(kccState).anyTimes();
    EasyMock.replay(mockKCC);
    
    KafkaCruiseControlServlet servlet = new KafkaCruiseControlServlet(mockKCC);
    KafkaCruiseControlServlet.GoalsAndRequirements goalsAndRequirements = 
        servlet.getGoalsAndRequirements(Collections.emptyList(),
                                        _dataFrom,
                                        false);
    
    assertEquals(new HashSet<>(goalsAndRequirements.goals()), new HashSet<>(_expectedGoalsToUse));
    if (_expectedRequirements != null) {
      assertEquals(_expectedRequirements.minRequiredNumSnapshotWindows(), 
                   goalsAndRequirements.requirements().minRequiredNumSnapshotWindows());
      assertEquals(_expectedRequirements.minMonitoredPartitionsPercentage(), 
                   goalsAndRequirements.requirements().minMonitoredPartitionsPercentage(), 0.0);
      assertEquals(_expectedRequirements.includeAllTopics(), 
                   goalsAndRequirements.requirements().includeAllTopics());
    } else {
      assertNull("The requirement should be null", goalsAndRequirements.requirements());
    }
  }

  /**
   * Generate the KCC state.
   */
  private KafkaCruiseControlState getState(int numReadyGoals, int totalGoals, int numValidWindows) {
    ExecutorState executorState = ExecutorState.noTaskInProgress();
    LoadMonitorState loadMonitorState = LoadMonitorState.running(numValidWindows, new TreeMap<>(),
                                                                 1, 10,
                                                                 Collections.emptyMap());
    Map<Goal, Boolean> goalReadiness = new HashMap<>();
    int i = 0;
    for (; i < numReadyGoals; i++) {
      goalReadiness.put(new MockGoal(i), true);
    }
    for (; i < totalGoals; i++) {
      goalReadiness.put(new MockGoal(i), false);
    }
    AnalyzerState analyzerState = new AnalyzerState(true, goalReadiness);
    return new KafkaCruiseControlState(executorState, loadMonitorState, analyzerState);
  }

  /**
   * Mock goal class to for testing.
   */
  private static class MockGoal implements Goal {
    private final int _id;
    private MockGoal(int id) {
      _id = id;
    }

    @Override
    public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics)
        throws KafkaCruiseControlException {
      return false;
    }

    @Override
    public boolean isProposalAcceptable(BalancingProposal proposal, ClusterModel clusterModel) {
      return false;
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
      return null;
    }

    @Override
    public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
      return null;
    }

    @Override
    public String name() {
      return Integer.toString(_id);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
  }
}
