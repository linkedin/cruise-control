/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.GoalBasedOptimizationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerState;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorState;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class KafkaCruiseControlServletDataFromTest {
  private static final ModelCompletenessRequirements FOR_AVAILABLE_WINDOWS =
      KafkaCruiseControlServletUtils.getRequirements(ParameterUtils.DataFrom.VALID_WINDOWS);
  private static final ModelCompletenessRequirements FOR_AVAILABLE_PARTITIONS =
      KafkaCruiseControlServletUtils.getRequirements(ParameterUtils.DataFrom.VALID_PARTITIONS);

  private final int _numReadyGoals;
  private final int _totalGoals;
  private final int _numValidWindows;
  private final ParameterUtils.DataFrom _dataFrom;
  private final List<String> _expectedGoalsToUse;
  private final ModelCompletenessRequirements _expectedRequirements;
  private final boolean _useReadyGoals;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    // all goals are ready, 1 valid window, with available windows.
    params.add(new Object[]{3, 3, 1, ParameterUtils.DataFrom.VALID_WINDOWS, Collections.emptyList(), null, true});
    // 2 out of 3 goals are ready, 1 valid window, with available windows.
    params.add(new Object[]{2, 3, 1, ParameterUtils.DataFrom.VALID_WINDOWS, Collections.emptyList(), FOR_AVAILABLE_WINDOWS, true});
    // all goals are ready, 1 valid window, with available partitions.
    params.add(new Object[]{3, 3, 1, ParameterUtils.DataFrom.VALID_PARTITIONS, Collections.emptyList(), FOR_AVAILABLE_PARTITIONS, true});
    // 2 out of 3 goals are ready, 1 valid window, with available partitions.
    params.add(new Object[]{2, 3, 1, ParameterUtils.DataFrom.VALID_PARTITIONS, Collections.emptyList(), FOR_AVAILABLE_PARTITIONS, true});
    // 2 out of 3 goals are ready, 0 valid window, with available windows.
    params.add(new Object[]{2, 3, 0, ParameterUtils.DataFrom.VALID_WINDOWS, Arrays.asList("0", "1"), null, true});
    // 2 out of 3 goals are ready, 0 valid window, with available windows, and does not use ready goals.
    params.add(new Object[]{2, 3, 0, ParameterUtils.DataFrom.VALID_WINDOWS, Collections.emptyList(), null, false});
    // No goal is ready, 0 valid window, with available windows.
    params.add(new Object[]{0, 3, 0, ParameterUtils.DataFrom.VALID_WINDOWS, Collections.emptyList(), null, true});
    return params;
  }

  public KafkaCruiseControlServletDataFromTest(int numReadyGoals, int totalGoals, int numValidWindows,
                                               ParameterUtils.DataFrom dataFrom,
                                               List<String> expectedGoalsToUse,
                                               ModelCompletenessRequirements expectedRequirements,
                                               boolean useReadyGoals) {
    _numReadyGoals = numReadyGoals;
    _totalGoals = totalGoals;
    _numValidWindows = numValidWindows;
    _dataFrom = dataFrom;
    _expectedGoalsToUse = expectedGoalsToUse;
    _expectedRequirements = expectedRequirements;
    _useReadyGoals = useReadyGoals;
  }

  @Test
  public void test() throws Exception {
    AsyncKafkaCruiseControl mockKCC = EasyMock.createMock(AsyncKafkaCruiseControl.class);
    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    HttpServletResponse response = EasyMock.createMock(HttpServletResponse.class);
    HttpSession session = EasyMock.createMock(HttpSession.class);
    EasyMock.expect(request.getSession()).andReturn(session).anyTimes();
    EasyMock.expect(request.getSession(false)).andReturn(session).anyTimes();
    EasyMock.expect(request.getMethod()).andReturn("GET").anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn("/test").anyTimes();
    EasyMock.expect(request.getParameterMap()).andReturn(Collections.emptyMap()).anyTimes();
    EasyMock.expect(request.getHeader(EasyMock.anyString())).andReturn(null).anyTimes();
    EasyMock.expect(request.getRemoteAddr()).andReturn("localhost").anyTimes();
    response.setHeader(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expect(session.getLastAccessedTime()).andReturn(Long.MAX_VALUE);
    CruiseControlState kccState = getState(_numReadyGoals, _totalGoals, _numValidWindows);
    OperationFuture kccStateFuture = new OperationFuture("test");
    kccStateFuture.complete(kccState);
    EasyMock.expect(mockKCC.state(EasyMock.anyObject()))
            .andReturn(kccStateFuture).anyTimes();
    EasyMock.expect(mockKCC.config()).andReturn(
        new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties())).anyTimes();
    EasyMock.replay(mockKCC, request, response, session);

    KafkaCruiseControlServlet servlet =
        new KafkaCruiseControlServlet(mockKCC, 10, 100, new MetricRegistry(), mockKCC.config());
    GoalBasedOptimizationParameters.GoalsAndRequirements goalsAndRequirements =
        servlet.getGoalsAndRequirements(request,
                                        response,
                                        Collections.emptyList(),
                                        _dataFrom,
                                        false,
                                        _useReadyGoals);

    assertEquals(new HashSet<>(_expectedGoalsToUse), new HashSet<>(goalsAndRequirements.goals()));
    if (_expectedRequirements != null) {
      assertEquals(_expectedRequirements.minRequiredNumWindows(),
                   goalsAndRequirements.requirements().minRequiredNumWindows());
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
  private CruiseControlState getState(int numReadyGoals, int totalGoals, int numValidWindows) {
    ExecutorState executorState = ExecutorState.noTaskInProgress(null, null);
    LoadMonitorState loadMonitorState = LoadMonitorState.running(numValidWindows, new TreeMap<>(),
                                                                 1, 10,
                                                                 Collections.emptyMap(),
                                                                 null);
    Map<Goal, Boolean> goalReadiness = new HashMap<>();
    int i = 0;
    for (; i < numReadyGoals; i++) {
      goalReadiness.put(new MockGoal(i), true);
    }
    for (; i < totalGoals; i++) {
      goalReadiness.put(new MockGoal(i), false);
    }
    AnalyzerState analyzerState = new AnalyzerState(true, goalReadiness);
    AnomalyDetectorState anomalyDetectorState = new AnomalyDetectorState(new HashMap<>(AnomalyType.cachedValues().size()), 10);
    return new CruiseControlState(executorState, loadMonitorState, analyzerState, anomalyDetectorState);
  }

  /**
   * Mock goal class to for testing.
   */
  private static class MockGoal implements Goal {
    private final int _id;
    private MockGoal(int id) {
      _id = id;
    }

    /**
     * @deprecated
     * Please use {@link #optimize(ClusterModel, Set, OptimizationOptions)} instead.
     */
    @Override
    public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics) {
      return false;
    }

    @Override
    public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions) {
      return false;
    }

    @Override
    public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
      return REPLICA_REJECT;
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
