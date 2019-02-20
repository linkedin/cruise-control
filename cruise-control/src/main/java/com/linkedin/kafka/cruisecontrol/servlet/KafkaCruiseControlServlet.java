/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddedOrRemovedBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AdminParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BaseParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.response.UserTaskState;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.EndPoint.*;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.hasValidParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.wantJSON;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.returnProgress;
import static javax.servlet.http.HttpServletResponse.SC_OK;


/**
 * The servlet for Kafka Cruise Control.
 */
public class KafkaCruiseControlServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlServlet.class);
  private final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
  private final KafkaCruiseControlConfig _config;
  private final UserTaskManager _userTaskManager;
  private final long _maxBlockMs;
  private final ThreadLocal<Integer> _asyncOperationStep;
  private final Map<EndPoint, Meter> _requestMeter = new HashMap<>();
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer = new HashMap<>();
  private final boolean _corsEnabled;

  public KafkaCruiseControlServlet(AsyncKafkaCruiseControl asynckafkaCruiseControl,
                                   long maxBlockMs,
                                   long sessionExpiryMs,
                                   MetricRegistry dropwizardMetricRegistry,
                                   KafkaCruiseControlConfig kafkaCruiseControlConfig) {
    _config = kafkaCruiseControlConfig;
    _corsEnabled = _config.getBoolean(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
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
    if (_corsEnabled) {
      response.setHeader("Access-Control-Allow-Origin",
                         _config.getString(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_ORIGIN_CONFIG));
      // This is required only as part of pre-flight response
      response.setHeader("Access-Control-Request-Method",
                         _config.getString(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG));
    }
  }

  /**
   * The GET requests can do the following:
   *
   * <pre>
   * 1. Bootstrap the load monitor (See {@link BootstrapParameters}).
   * 2. Train the Kafka Cruise Control linear regression model (See {@link TrainParameters}).
   * 3. Get the cluster load (See {@link ClusterLoadParameters}).
   * 4. Get the partition load (See {@link PartitionLoadParameters}).
   * 5. Get an optimization proposal (See {@link ProposalsParameters}).
   * 6. Get the state of Cruise Control (See {@link CruiseControlStateParameters}).
   * 7. Get the Kafka cluster state (See {@link KafkaClusterStateParameters}).
   * 8. Get active user tasks (See {@link UserTasksParameters}).
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = getValidEndpoint(request, response);
      if (endPoint != null && hasValidParameters(request, response)) {
        _requestMeter.get(endPoint).mark();
        switch (endPoint) {
          case BOOTSTRAP:
            syncRequest(() -> new BootstrapParameters(request), _asyncKafkaCruiseControl::bootstrapLoadMonitor,
                        response, _successfulRequestExecutionTimer.get(endPoint));
            break;
          case TRAIN:
            syncRequest(() -> new TrainParameters(request), _asyncKafkaCruiseControl::trainLoadModel,
                        response, _successfulRequestExecutionTimer.get(endPoint));
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
            syncRequest(() -> new KafkaClusterStateParameters(request), _asyncKafkaCruiseControl::kafkaClusterState,
                        response, _successfulRequestExecutionTimer.get(endPoint));
            break;
          case USER_TASKS:
            syncRequest(() -> new UserTasksParameters(request), this::userTaskState,
                        response, _successfulRequestExecutionTimer.get(endPoint));
            break;
          default:
            throw new UserRequestException("Invalid URL for GET");
        }
      }
    } catch (UserRequestException ure) {
      String errorMessage = handleUserRequestException(ure, request, response);
      LOG.error(errorMessage, ure);
    } catch (Exception e) {
      String errorMessage = handleException(e, request, response);
      LOG.error(errorMessage, e);
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
   * 1. Decommission a broker (See {@link AddedOrRemovedBrokerParameters}).
   * 2. Add a broker (See {@link AddedOrRemovedBrokerParameters}).
   * 3. Trigger a workload balance (See {@link RebalanceParameters}).
   * 4. Stop the proposal execution (See {@link BaseParameters}).
   * 5. Pause metrics sampling (See {@link PauseResumeParameters}).
   * 6. Resume metrics sampling (See {@link PauseResumeParameters}).
   * 7. Demote a broker (See {@link DemoteBrokerParameters}).
   * 8. Admin operations on Cruise Control (See {@link AdminParameters}).
   * </pre>
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = getValidEndpoint(request, response);
      if (endPoint != null && hasValidParameters(request, response)) {
        _requestMeter.get(endPoint).mark();
        switch (endPoint) {
          case ADD_BROKER:
          case REMOVE_BROKER:
            addOrRemoveBroker(request, response, endPoint);
            break;
          case REBALANCE:
            rebalance(request, response);
            break;
          case STOP_PROPOSAL_EXECUTION:
            syncRequest(() -> new BaseParameters(request), _asyncKafkaCruiseControl::stopProposalExecution,
                        response, _successfulRequestExecutionTimer.get(endPoint));
            break;
          case PAUSE_SAMPLING:
            syncRequest(() -> new PauseResumeParameters(request), _asyncKafkaCruiseControl::pauseLoadMonitorActivity,
                        response, _successfulRequestExecutionTimer.get(endPoint));
            break;
          case RESUME_SAMPLING:
            syncRequest(() -> new PauseResumeParameters(request), _asyncKafkaCruiseControl::resumeLoadMonitorActivity,
                        response, _successfulRequestExecutionTimer.get(endPoint));
            break;
          case DEMOTE_BROKER:
            demoteBroker(request, response);
            break;
          case ADMIN:
            syncRequest(() -> new AdminParameters(request), _asyncKafkaCruiseControl::handleAdminRequest,
                        response, _successfulRequestExecutionTimer.get(endPoint));
            break;
          default:
            throw new UserRequestException("Invalid URL for POST");
        }
      }
    } catch (UserRequestException ure) {
      String errorMessage = handleUserRequestException(ure, request, response);
      LOG.error(errorMessage, ure);
    } catch (Exception e) {
      String errorMessage = handleException(e, request, response);
      LOG.error(errorMessage, e);
    } finally {
      try {
        response.getOutputStream().close();
      } catch (IOException e) {
        LOG.warn("Error closing output stream: ", e);
      }
    }
  }

  private UserTaskState userTaskState() {
    return new UserTaskState(_userTaskManager.getActiveUserTasks(), _userTaskManager.getCompletedUserTasks());
  }

  private void getClusterLoad(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new ClusterLoadParameters(request),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.getBrokerStats(parameters)),
                 request, response);
  }

  private void getPartitionLoad(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new PartitionLoadParameters(request),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.partitionLoadState(parameters)),
                 request, response);
  }

  private void getProposals(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new ProposalsParameters(request),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.getProposals(parameters)),
                 request, response);
  }

  private void getState(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new CruiseControlStateParameters(request),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.state(parameters)),
                 request, response);
  }

  private void rebalance(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new RebalanceParameters(request, _config),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.rebalance(parameters, uuid)),
                 request, response);
  }

  private void demoteBroker(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new DemoteBrokerParameters(request, _config),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.demoteBrokers(uuid, parameters)),
                 request, response);
  }

  private void addOrRemoveBroker(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws IOException, ExecutionException, InterruptedException {
    Function<AddedOrRemovedBrokerParameters, Function<String, OperationFuture>> function;
    if (endPoint == ADD_BROKER) {
      function = parameters -> (uuid -> _asyncKafkaCruiseControl.addBrokers(parameters, uuid));
    } else {
      function = parameters -> (uuid -> _asyncKafkaCruiseControl.decommissionBrokers(parameters, uuid));
    }

    asyncRequest(() -> new AddedOrRemovedBrokerParameters(request, _config), function, request, response);
  }

  /**
   * Handle async request and populate the response.
   *
   * @param paramSupplier Supplier to get the request parameters.
   * @param function Function that generates the function to pass to getAndMaybeReturnProgress using request parameters.
   * @param request Http servlet request.
   * @param response Http servlet response.
   * @param <P> Type corresponding to the request parameters.
   */
  private <P extends CruiseControlParameters> void asyncRequest(Supplier<P> paramSupplier,
                                                                Function<P, Function<String, OperationFuture>> function,
                                                                HttpServletRequest request,
                                                                HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    P parameters = paramSupplier.get();
    if (parameters.parseParameters(response)) {
      LOG.warn("Failed to parse parameters: {} for async request: {}.", request.getParameterMap(), request.getPathInfo());
      return;
    }

    CruiseControlResponse ccResponse = getAndMaybeReturnProgress(request, response, function.apply(parameters));
    if (ccResponse == null) {
      LOG.info("Computation is in progress for async request: {}.", request.getPathInfo());
      return;
    }

    ccResponse.writeSuccessResponse(parameters, response);
    LOG.info("Computation is completed for async request: {}.", request.getPathInfo());
  }

  private CruiseControlResponse getAndMaybeReturnProgress(HttpServletRequest request,
                                                          HttpServletResponse response,
                                                          Function<String, OperationFuture> function)
      throws ExecutionException, InterruptedException, IOException {
    int step = _asyncOperationStep.get();
    List<OperationFuture> futures = _userTaskManager.getOrCreateUserTask(request, response, function, step, true);
    _asyncOperationStep.set(step + 1);
    try {
      return futures.get(step).get(_maxBlockMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      returnProgress(response, futures, wantJSON(request), _config);
      return null;
    }
  }
}
