/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddedOrRemovedBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.GoalBasedOptimizationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BaseParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.response.BootstrapResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import com.linkedin.kafka.cruisecontrol.servlet.response.PauseSamplingResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.ResumeSamplingResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.StopProposalExecutionResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.TrainResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.UserTaskState;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.EndPoint.*;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.hasValidParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.wantJSON;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DataFrom;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.returnProgress;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.writeErrorResponse;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;


/**
 * The servlet for Kafka Cruise Control.
 */
public class KafkaCruiseControlServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlServlet.class);
  private static final Logger ACCESS_LOG = LoggerFactory.getLogger("CruiseControlPublicAccessLogger");
  private final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
  private final UserTaskManager _userTaskManager;
  private final long _maxBlockMs;
  private final ThreadLocal<Integer> _asyncOperationStep;
  private final Map<EndPoint, Meter> _requestMeter = new HashMap<>();
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer = new HashMap<>();

  public KafkaCruiseControlServlet(AsyncKafkaCruiseControl asynckafkaCruiseControl,
                                   long maxBlockMs,
                                   long sessionExpiryMs,
                                   MetricRegistry dropwizardMetricRegistry) {
    _asyncKafkaCruiseControl = asynckafkaCruiseControl;
    KafkaCruiseControlConfig config = asynckafkaCruiseControl.config();
    _userTaskManager = new UserTaskManager(sessionExpiryMs, config.getInt(KafkaCruiseControlConfig.MAX_ACTIVE_USER_TASKS_CONFIG),
                                           config.getLong(KafkaCruiseControlConfig.COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG),
                                           config.getInt(KafkaCruiseControlConfig.MAX_CACHED_COMPLETED_USER_TASKS_CONFIG),
                                           dropwizardMetricRegistry, _successfulRequestExecutionTimer);
    _maxBlockMs = maxBlockMs;
    _asyncOperationStep = new ThreadLocal<>();
    _asyncOperationStep.set(0);

    for (EndPoint endpoint : EndPoint.cachedValues()) {
      _requestMeter.put(endpoint, dropwizardMetricRegistry.meter(
          MetricRegistry.name("KafkaCruiseControlServlet", endpoint.name() + "-request-rate")));
      _successfulRequestExecutionTimer.put(endpoint, dropwizardMetricRegistry.timer(
          MetricRegistry.name("KafkaCruiseControlServlet", endpoint.name() + "-successful-request-execution-timer")));
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    _userTaskManager.close();
  }

  /**
   * OPTIONS request takes care of CORS applications
   *
   * https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS#Examples_of_access_control_scenarios
   */
  protected void doOptions(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(SC_OK);
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
  }

  /**
   * The GET requests can do the following:
   *
   * NOTE: ADD json=true to the query parameters to get 200/OK response in JSON format.
   *
   * <pre>
   * 1. Bootstrap the load monitor
   *    RANGE MODE:
   *      GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
   *    SINCE MODE:
   *      GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]
   *    RECENT MODE:
   *      GET /kafkacruisecontrol/bootstrap
   *
   * 2. Train the Kafka Cruise Control linear regression model. The trained model will only be used if
   *    use.linear.regression.model is set to true.
   *    GET /kafkacruisecontrol/train?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
   *
   * 3. Get the cluster load
   *    GET /kafkacruisecontrol/load?time=[TIMESTAMP]
   *
   * 4. Get the partition load sorted by the utilization of a given resource and filtered by given topic regular expression
   *    and partition number/range
   *    GET /kafkacruisecontrol/partition_load?resource=[RESOURCE]&amp;start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
   *    &amp;topic=[topic]&amp;partition=[partition/start_partition-end_partition]
   *
   * 5. Get an optimization proposal
   *    GET /kafkacruisecontrol/proposals?verbose=[ENABLE_VERBOSE]&amp;ignore_proposal_cache=[true/false]
   *    &amp;goals=[goal1,goal2...]&amp;data_from=[valid_windows/valid_partitions]&amp;excluded_topics=[pattern]
   *    &amp;use_ready_default_goals=[true/false]
   *
   * 6. query the state of Kafka Cruise Control
   *    GET /kafkacruisecontrol/state
   *
   * 7. query the Kafka cluster state
   *    GET /kafkacruisecontrol/kafka_cluster_state
   *
   * 8. query to get active user tasks
   *    GET /kafkacruisecontrol/user_tasks
   *
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    ACCESS_LOG.info("Received {}, {} from {}", urlEncode(request.toString()),
                    urlEncode(request.getRequestURL().toString()),
                    getClientIpAddress(request));
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = getValidEndpoint(request, response);
      if (endPoint != null && hasValidParameters(request, response)) {
        _requestMeter.get(endPoint).mark();
        long requestExecutionStartTime = System.nanoTime();
        switch (endPoint) {
          case BOOTSTRAP:
            bootstrap(request, response);
            _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
            break;
          case TRAIN:
            train(request, response);
            _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
            break;
          case LOAD:
            getClusterLoad(request, response);
            break;
          case PARTITION_LOAD:
            getPartitionLoad(request, response);
            break;
          case PROPOSALS:
            getProposals(request, response);
            break;
          case STATE:
            getState(request, response);
            break;
          case KAFKA_CLUSTER_STATE:
            getKafkaClusterState(request, response);
            _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
            break;
          case USER_TASKS:
            getUserTaskState(request, response);
            _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
            break;
          default:
            throw new UserRequestException("Invalid URL for GET");
        }
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      handleUserRequestException(ure, request, response);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String errorMessage = String.format("Error processing GET request '%s' due to '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      writeErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request));
    } finally {
      try {
        response.getOutputStream().close();
      } catch (IOException e) {
        LOG.warn("Error closing output stream: ", e);
      }
    }
  }

  /**
   * The POST method allows user to perform the following actions:
   *
   * <pre>
   * 1. Decommission a broker.
   *    POST /kafkacruisecontrol/remove_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]&amp;throttle_removed_broker=[true/false]&amp;goals=[goal1,goal2...]
   *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]
   *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]
   *
   * 2. Add a broker
   *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]&amp;throttle_added_broker=[true/false]&amp;goals=[goal1,goal2...]
   *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]
   *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]
   *
   * 3. Trigger a workload balance.
   *    POST /kafkacruisecontrol/rebalance?dryRun=[true/false]&amp;force=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
   *    &amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]&amp;json=[true/false]
   *    &amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]
   *
   * 4. Stop the proposal execution.
   *    POST /kafkacruisecontrol/stop_proposal_execution?json=[true/false]
   *
   * 5. Pause metrics sampling. (RUNNING -&gt; PAUSED).
   *    POST /kafkacruisecontrol/pause_sampling?json=[true/false]
   *
   * 6. Resume metrics sampling. (PAUSED -&gt; RUNNING).
   *    POST /kafkacruisecontrol/resume_sampling?json=[true/false]
   *
   * 7. Demote a broker
   *    POST /kafkacruisecontrol/demote_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]&amp;concurrent_leader_movements=[true/false]
   *    &amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]&amp;excluded_topics=[pattern]
   *
   * 8. Admin.
   *    POST /kafkacruisecontrol/admin?json=[true/false]
   *
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    ACCESS_LOG.info("Received {}, {} from {}", urlEncode(request.toString()),
                    urlEncode(request.getRequestURL().toString()),
                    getClientIpAddress(request));
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = getValidEndpoint(request, response);
      if (endPoint != null && hasValidParameters(request, response)) {
        _requestMeter.get(endPoint).mark();

        long requestExecutionStartTime = System.nanoTime();
        switch (endPoint) {
          case ADD_BROKER:
          case REMOVE_BROKER:
            addOrRemoveBroker(request, response, endPoint);
            break;
          case REBALANCE:
            rebalance(request, response);
            break;
          case STOP_PROPOSAL_EXECUTION:
            stopProposalExecution(request, response);
            _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
            break;
          case PAUSE_SAMPLING:
            pauseSampling(request, response);
            _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
            break;
          case RESUME_SAMPLING:
            resumeSampling(request, response);
            _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
            break;
          case DEMOTE_BROKER:
            demoteBroker(request, response);
            break;
          default:
            throw new UserRequestException("Invalid URL for POST");
        }
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      handleUserRequestException(ure, request, response);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String errorMessage = String.format("Error processing POST request '%s' due to: '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      writeErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request));
    } finally {
      try {
        response.getOutputStream().close();
      } catch (IOException e) {
        LOG.warn("Error closing output stream: ", e);
      }
    }
  }

  private void bootstrap(HttpServletRequest request, HttpServletResponse response) throws Exception {
    BootstrapParameters parameters = new BootstrapParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return;
    }

    _asyncKafkaCruiseControl.bootstrapLoadMonitor(parameters);
    new BootstrapResult().writeSuccessResponse(parameters, response);
  }

  private void train(HttpServletRequest request, HttpServletResponse response) throws Exception {
    TrainParameters parameters = new TrainParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return;
    }

    _asyncKafkaCruiseControl.trainLoadModel(parameters);
    new TrainResult().writeSuccessResponse(parameters, response);
  }

  private boolean getClusterLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    ClusterLoadParameters parameters = new ClusterLoadParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return true;
    }

    CruiseControlResponse cachedBrokerStats = _asyncKafkaCruiseControl.cachedBrokerLoadStats(parameters);
    if (cachedBrokerStats != null) {
      // Get the cached broker stats.
      cachedBrokerStats.writeSuccessResponse(parameters, response);
    } else {
      // Get the broker stats asynchronously.
      CruiseControlResponse nonCachedBrokerStats =
          getAndMaybeReturnProgress(request, response, uuid -> _asyncKafkaCruiseControl.getBrokerStats(parameters));
      if (nonCachedBrokerStats == null) {
        return false;
      }
      nonCachedBrokerStats.writeSuccessResponse(parameters, response);
    }
    return true;
  }

  private boolean getPartitionLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    PartitionLoadParameters parameters = new PartitionLoadParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return true;
    }
    // Get cluster model asynchronously.
    CruiseControlResponse kafkaPartitionLoadState = getAndMaybeReturnProgress(request, response,
                                                                              uuid -> _asyncKafkaCruiseControl.partitionLoadState(parameters));
    if (kafkaPartitionLoadState == null) {
      return false;
    }

    kafkaPartitionLoadState.writeSuccessResponse(parameters, response);
    return true;
  }

  private boolean getProposals(HttpServletRequest request, HttpServletResponse response) throws Exception {
    ProposalsParameters parameters = new ProposalsParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return true;
    }

    GoalBasedOptimizationParameters.GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(request, response, parameters.goals(), parameters.dataFrom(),
                                parameters.ignoreProposalCache(), parameters.useReadyDefaultGoals());
    if (goalsAndRequirements == null) {
      return false;
    }
    // Get the optimization result asynchronously.
    CruiseControlResponse optimizationResult = getAndMaybeReturnProgress(
        request, response, uuid -> _asyncKafkaCruiseControl.getOptimizationProposals(goalsAndRequirements.goals(),
                                                                                     goalsAndRequirements.requirements(),
                                                                                     parameters));
    if (optimizationResult == null) {
      return false;
    }

    optimizationResult.writeSuccessResponse(parameters, response);
    return true;
  }

  private void getKafkaClusterState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    KafkaClusterStateParameters parameters = new KafkaClusterStateParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return;
    }

    _asyncKafkaCruiseControl.kafkaClusterState().writeSuccessResponse(parameters, response);
  }

  private boolean getState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    CruiseControlStateParameters parameters = new CruiseControlStateParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return true;
    }

    CruiseControlResponse state = getAndMaybeReturnProgress(request, response,
                                                            uuid -> _asyncKafkaCruiseControl.state(parameters));
    if (state == null) {
      return false;
    }

    state.writeSuccessResponse(parameters, response);
    return true;
  }

  private boolean addOrRemoveBroker(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws Exception {
    AddedOrRemovedBrokerParameters parameters = new AddedOrRemovedBrokerParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return true;
    }

    GoalBasedOptimizationParameters.GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(request, response, parameters.goals(), parameters.dataFrom(), false, parameters.useReadyDefaultGoals());
    if (goalsAndRequirements == null) {
      return false;
    }
    // Get proposals asynchronously.
    CruiseControlResponse optimizationResult;
    if (endPoint == ADD_BROKER) {
      optimizationResult =
          getAndMaybeReturnProgress(request, response,
                                    uuid -> _asyncKafkaCruiseControl.addBrokers(goalsAndRequirements.goals(),
                                                                              goalsAndRequirements.requirements(),
                                                                              parameters,
                                                                              uuid));
    } else {
      optimizationResult =
          getAndMaybeReturnProgress(request, response,
                                    uuid -> _asyncKafkaCruiseControl.decommissionBrokers(goalsAndRequirements.goals(),
                                                                                       goalsAndRequirements.requirements(),
                                                                                       parameters,
                                                                                         uuid));
    }
    if (optimizationResult == null) {
      return false;
    }

    optimizationResult.writeSuccessResponse(parameters, response);
    return true;
  }

  private boolean rebalance(HttpServletRequest request, HttpServletResponse response) throws Exception {
    RebalanceParameters parameters = new RebalanceParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return true;
    }

    GoalBasedOptimizationParameters.GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(request, response, parameters.goals(), parameters.dataFrom(), false, parameters.useReadyDefaultGoals());
    if (goalsAndRequirements == null) {
      return false;
    }
    CruiseControlResponse optimizationResult =
        getAndMaybeReturnProgress(request, response,
                                  uuid -> _asyncKafkaCruiseControl.rebalance(goalsAndRequirements.goals(),
                                                                           goalsAndRequirements.requirements(),
                                                                           parameters,
                                                                           uuid));
    if (optimizationResult == null) {
      return false;
    }

    optimizationResult.writeSuccessResponse(parameters, response);
    return true;
  }

  private boolean demoteBroker(HttpServletRequest request, HttpServletResponse response) throws Exception {
    DemoteBrokerParameters parameters = new DemoteBrokerParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return true;
    }

    // Get proposals asynchronously.
    CruiseControlResponse optimizationResult =
        getAndMaybeReturnProgress(request, response, uuid -> _asyncKafkaCruiseControl.demoteBrokers(uuid, parameters));
    if (optimizationResult == null) {
      return false;
    }

    optimizationResult.writeSuccessResponse(parameters, response);
    return true;
  }

  private void stopProposalExecution(HttpServletRequest request, HttpServletResponse response) throws IOException {
    BaseParameters parameters = new BaseParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return;
    }
    _asyncKafkaCruiseControl.stopProposalExecution();
    new StopProposalExecutionResult().writeSuccessResponse(parameters, response);
  }

  private void pauseSampling(HttpServletRequest request, HttpServletResponse response) throws IOException {
    BaseParameters parameters = new BaseParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return;
    }
    _asyncKafkaCruiseControl.pauseLoadMonitorActivity();
    new PauseSamplingResult().writeSuccessResponse(parameters, response);
  }

  private void resumeSampling(HttpServletRequest request, HttpServletResponse response) throws IOException {
    BaseParameters parameters = new BaseParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return;
    }
    _asyncKafkaCruiseControl.resumeLoadMonitorActivity();
    new ResumeSamplingResult().writeSuccessResponse(parameters, response);
  }

  private CruiseControlResponse getAndMaybeReturnProgress(HttpServletRequest request,
                                                          HttpServletResponse response,
                                                          Function<String, OperationFuture> function)
      throws ExecutionException, InterruptedException, IOException {
    int step = _asyncOperationStep.get();
    List<OperationFuture> futures = _userTaskManager.getOrCreateUserTask(request, response, function, step);
    _asyncOperationStep.set(step + 1);
    try {
      return futures.get(step).get(_maxBlockMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      returnProgress(response, futures, wantJSON(request));
      return null;
    }
  }

  // package private for testing.
  GoalBasedOptimizationParameters.GoalsAndRequirements getGoalsAndRequirements(HttpServletRequest request,
                                                                               HttpServletResponse response,
                                                                               List<String> userProvidedGoals,
                                                                               DataFrom dataFrom,
                                                                               boolean ignoreCache,
                                                                               boolean useReadyDefaultGoals) throws Exception {
    if (!userProvidedGoals.isEmpty() || dataFrom == DataFrom.VALID_PARTITIONS) {
      return new GoalBasedOptimizationParameters.GoalsAndRequirements(userProvidedGoals, getRequirements(dataFrom));
    }

    CruiseControlStateParameters parameters = new CruiseControlStateParameters(null);
    parameters.setSubstates(new HashSet<>(Arrays.asList(CruiseControlState.SubState.ANALYZER,
                                                        CruiseControlState.SubState.MONITOR)));

    CruiseControlResponse state = getAndMaybeReturnProgress(request, response,
                                                            uuid -> _asyncKafkaCruiseControl.state(parameters));
    if (state == null) {
      return null;
    }
    int availableWindows = ((CruiseControlState) state).monitorState().numValidWindows();
    List<String> allGoals = new ArrayList<>();
    List<String> readyGoals = new ArrayList<>();
    ((CruiseControlState) state).analyzerState().readyGoals().forEach((goal, ready) -> {
      allGoals.add(goal.name());
      if (ready) {
        readyGoals.add(goal.name());
      }
    });
    if (allGoals.size() == readyGoals.size()) {
      // If all the goals work, use it.
      return new GoalBasedOptimizationParameters.GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(), null);
    } else if (availableWindows > 0) {
      // If some valid windows are available, use it.
      return new GoalBasedOptimizationParameters.GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(), getRequirements(dataFrom));
    } else if (useReadyDefaultGoals && readyGoals.size() > 0) {
      // If no window is valid but some goals are ready, use them if using ready goals is permitted.
      return new GoalBasedOptimizationParameters.GoalsAndRequirements(readyGoals, null);
    } else {
      // Ok, use default setting and let it throw exception.
      return new GoalBasedOptimizationParameters.GoalsAndRequirements(Collections.emptyList(), null);
    }
  }

  private void getUserTaskState(HttpServletRequest request, HttpServletResponse response) throws IOException {
    UserTasksParameters parameters = new UserTasksParameters(request);
    if (parameters.parseParameters(response)) {
      // Failed to parse parameters.
      return;
    }

    UserTaskState userTaskState = new UserTaskState(_userTaskManager.getActiveUserTasks(),
                                                    _userTaskManager.getCompletedUserTasks());
    userTaskState.writeSuccessResponse(parameters, response);
  }
}
