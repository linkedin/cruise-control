/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddedOrRemovedBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AdminParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewBoardParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.RequestInfo;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;
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
  private final boolean _twoStepVerification;
  private final Purgatory _purgatory;

  public KafkaCruiseControlServlet(AsyncKafkaCruiseControl asynckafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
    _config = asynckafkaCruiseControl.config();
    _corsEnabled = _config.getBoolean(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
    _asyncKafkaCruiseControl = asynckafkaCruiseControl;
    _twoStepVerification = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _purgatory = _twoStepVerification ? new Purgatory(_config) : null;
    _userTaskManager = new UserTaskManager(_config, dropwizardMetricRegistry, _successfulRequestExecutionTimer, _purgatory);
    _maxBlockMs = _config.getLong(KafkaCruiseControlConfig.WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS);
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
    _purgatory.close();
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
      response.setHeader("Access-Control-Allow-Methods",
                         _config.getString(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG));
      response.setHeader("Access-Control-Allow-Headers",
                         _config.getString(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG));
      response.setHeader("Access-Control-Max-Age", String.valueOf(1728000));
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
   * 9. Get reviews in the review board (See {@link ReviewBoardParameters}).
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = getValidEndpoint(request, response, _config);
      if (endPoint != null && hasValidParameters(request, response, _config)) {
        _requestMeter.get(endPoint).mark();
        switch (endPoint) {
          case BOOTSTRAP:
            syncRequest(() -> new BootstrapParameters(request, _config), _asyncKafkaCruiseControl::bootstrapLoadMonitor,
                        request, response, endPoint);
            break;
          case TRAIN:
            syncRequest(() -> new TrainParameters(request, _config), _asyncKafkaCruiseControl::trainLoadModel,
                        request, response, endPoint);
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
            syncRequest(() -> new KafkaClusterStateParameters(request, _config), _asyncKafkaCruiseControl::kafkaClusterState,
                        request, response, endPoint);
            break;
          case USER_TASKS:
            syncRequest(() -> new UserTasksParameters(request, _config), this::userTaskState, request, response, endPoint);
            break;
          case REVIEW_BOARD:
            if (!_twoStepVerification) {
              throw new ConfigException(String.format("Attempt to access %s endpoint without enabling '%s' config.",
                                                      endPoint, KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG));
            }
            syncRequest(() -> new ReviewBoardParameters(request, _config), this::handleReviewBoardRequest, request, response, endPoint);
            break;
          default:
            throw new UserRequestException("Invalid URL for GET");
        }
      }
    } catch (UserRequestException ure) {
      String errorMessage = handleUserRequestException(ure, request, response, _config);
      LOG.error(errorMessage, ure);
    } catch (ConfigException ce) {
      String errorMessage = handleConfigException(ce, request, response, _config);
      LOG.error(errorMessage, ce);
    } catch (Exception e) {
      String errorMessage = handleException(e, request, response, _config);
      LOG.error(errorMessage, e);
    } finally {
      try {
        response.getOutputStream().close();
      } catch (IOException e) {
        LOG.warn("Error closing output stream: ", e);
      }
    }
  }

  private void sanityCheckSubmittedRequest(HttpServletRequest request, RequestInfo requestInfo) {
    if (requestInfo.accessToAlreadySubmittedRequest()
        && _userTaskManager.getUserTaskByUserTaskId(_userTaskManager.getUserTaskId(request), request) == null) {
     throw new UserRequestException(
         String.format("Attempt to start a new user task with an already submitted review. If you are trying to retrieve"
                       + " the result of a submitted execution, please use its UUID in your request header via %s flag."
                       + " If you are starting a new execution with the same parameters, please submit a new review "
                       + "request and get approval for it.", UserTaskManager.USER_TASK_HEADER_NAME));
    }
  }

  @SuppressWarnings("unchecked")
  private <P extends CruiseControlParameters> P maybeAddToPurgatory(HttpServletRequest request,
                                                                    HttpServletResponse response,
                                                                    Supplier<P> paramSupplier) throws IOException {
    Integer reviewId = ParameterUtils.reviewId(request, _twoStepVerification);
    if (reviewId != null) {
      // Submit the request with reviewId that should already be in the purgatory associated with the request endpoint.
      RequestInfo requestInfo = _purgatory.submit(reviewId, request);
      // Ensure that if the request has already been submitted, the user is not attempting to create another user task
      // with the same parameters and endpoint.
      sanityCheckSubmittedRequest(request, requestInfo);

      return (P) requestInfo.parameters();
    } else {
      P parameters = paramSupplier.get();
      if (!parameters.parseParameters(response)) {
        // Add request to purgatory and return ReviewResult.
        ReviewResult reviewResult = _purgatory.addRequest(request, parameters);
        reviewResult.writeSuccessResponse(parameters, response);
        LOG.info("Added request {} (parameters: {}) to purgatory.", request.getPathInfo(), request.getParameterMap());
      }

      return null;
    }
  }

  private <P extends CruiseControlParameters> P evaluateReviewableParams(HttpServletRequest request,
                                                                         HttpServletResponse response,
                                                                         Supplier<P> paramSupplier)
      throws IOException {
    // Do not add to the purgatory if the two-step verification is disabled.
    return !_twoStepVerification ? paramSupplier.get() : maybeAddToPurgatory(request, response, paramSupplier);
  }

  /**
   * The POST method allows user to perform the following actions:
   *
   * <pre>
   * 1. Decommission a broker (See {@link AddedOrRemovedBrokerParameters}).
   * 2. Add a broker (See {@link AddedOrRemovedBrokerParameters}).
   * 3. Trigger a workload balance (See {@link RebalanceParameters}).
   * 4. Stop the proposal execution (See {@link StopProposalParameters}).
   * 5. Pause metrics sampling (See {@link PauseResumeParameters}).
   * 6. Resume metrics sampling (See {@link PauseResumeParameters}).
   * 7. Demote a broker (See {@link DemoteBrokerParameters}).
   * 8. Admin operations on Cruise Control (See {@link AdminParameters}).
   * 9. Review requests for two-step verification (See {@link ReviewParameters}).
   * </pre>
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = getValidEndpoint(request, response, _config);
      if (endPoint != null && hasValidParameters(request, response, _config)) {
        _requestMeter.get(endPoint).mark();
        CruiseControlParameters reviewableParams;
        switch (endPoint) {
          case ADD_BROKER:
          case REMOVE_BROKER:
            reviewableParams = evaluateReviewableParams(request, response,
                                                        () -> new AddedOrRemovedBrokerParameters(request, _config));
            if (reviewableParams != null) {
              addOrRemoveBroker(request, response, endPoint, () -> (AddedOrRemovedBrokerParameters) reviewableParams);
            }
            break;
          case REBALANCE:
            reviewableParams = evaluateReviewableParams(request, response, () -> new RebalanceParameters(request, _config));
            if (reviewableParams != null) {
              rebalance(request, response, () -> (RebalanceParameters) reviewableParams);
            }
            break;
          case STOP_PROPOSAL_EXECUTION:
            reviewableParams = evaluateReviewableParams(request, response,
                                                        () -> new StopProposalParameters(request, _config));
            if (reviewableParams != null) {
              syncRequest(() -> (StopProposalParameters) reviewableParams, _asyncKafkaCruiseControl::stopProposalExecution,
                          request, response, endPoint);
            }
            break;
          case PAUSE_SAMPLING:
            reviewableParams = evaluateReviewableParams(request, response,
                                                        () -> new PauseResumeParameters(request, _config));
            if (reviewableParams != null) {
              syncRequest(() -> (PauseResumeParameters) reviewableParams, _asyncKafkaCruiseControl::pauseLoadMonitorActivity,
                          request, response, endPoint);
            }
            break;
          case RESUME_SAMPLING:
            reviewableParams = evaluateReviewableParams(request, response,
                                                        () -> new PauseResumeParameters(request, _config));
            if (reviewableParams != null) {
              syncRequest(() -> (PauseResumeParameters) reviewableParams, _asyncKafkaCruiseControl::resumeLoadMonitorActivity,
                          request, response, endPoint);
            }
            break;
          case DEMOTE_BROKER:
            reviewableParams = evaluateReviewableParams(request, response, () -> new DemoteBrokerParameters(request, _config));
            if (reviewableParams != null) {
              demoteBroker(request, response, () -> (DemoteBrokerParameters) reviewableParams);
            }
            break;
          case ADMIN:
            reviewableParams = evaluateReviewableParams(request, response,
                                                        () -> new AdminParameters(request, _config));
            if (reviewableParams != null) {
              syncRequest(() -> (AdminParameters) reviewableParams, _asyncKafkaCruiseControl::handleAdminRequest,
                          request, response, endPoint);
            }
            break;
          case REVIEW:
            if (!_twoStepVerification) {
              throw new ConfigException(String.format("Attempt to access %s endpoint without enabling '%s' config.",
                                                      endPoint, KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG));
            }
            syncRequest(() -> new ReviewParameters(request, _config), this::handleReviewRequest, request, response, endPoint);
            break;
          default:
            throw new UserRequestException("Invalid URL for POST");
        }
      }
    } catch (UserRequestException ure) {
      String errorMessage = handleUserRequestException(ure, request, response, _config);
      LOG.error(errorMessage, ure);
    } catch (ConfigException ce) {
      String errorMessage = handleConfigException(ce, request, response, _config);
      LOG.error(errorMessage, ce);
    } catch (Exception e) {
      String errorMessage = handleException(e, request, response, _config);
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
   * Get the user task state.
   *
   * @param parameters User task state parameters (not used -- added for standardization and for extensibility).
   * @return User task state.
   */
  private UserTaskState userTaskState(UserTasksParameters parameters) {
    return new UserTaskState(_userTaskManager, _config);
  }

  private ReviewResult handleReviewRequest(ReviewParameters parameters) {
    return _purgatory.applyReview(parameters.reviewRequests(), parameters.reason());
  }

  private ReviewResult handleReviewBoardRequest(ReviewBoardParameters parameters) {
    return _purgatory.reviewBoard(parameters.reviewIds());
  }

  private void getClusterLoad(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new ClusterLoadParameters(request, _config),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.getBrokerStats(parameters)),
                 request, response);
  }

  private void getPartitionLoad(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new PartitionLoadParameters(request, _config),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.partitionLoadState(parameters)),
                 request, response);
  }

  private void getProposals(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new ProposalsParameters(request, _config),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.getProposals(parameters)),
                 request, response);
  }

  private void getState(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(() -> new CruiseControlStateParameters(request, _config),
                 parameters -> (uuid -> _asyncKafkaCruiseControl.state(parameters)),
                 request, response);
  }

  private void rebalance(HttpServletRequest request, HttpServletResponse response, Supplier<RebalanceParameters> paramSupplier)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(paramSupplier, parameters -> (uuid -> _asyncKafkaCruiseControl.rebalance(parameters, uuid)),
                 request, response);
  }

  private void demoteBroker(HttpServletRequest request, HttpServletResponse response, Supplier<DemoteBrokerParameters> paramSupplier)
      throws IOException, ExecutionException, InterruptedException {
    asyncRequest(paramSupplier, parameters -> (uuid -> _asyncKafkaCruiseControl.demoteBrokers(uuid, parameters)),
                 request, response);
  }

  private void addOrRemoveBroker(HttpServletRequest request,
                                 HttpServletResponse response,
                                 EndPoint endPoint,
                                 Supplier<AddedOrRemovedBrokerParameters> paramSupplier)
      throws IOException, ExecutionException, InterruptedException {
    Function<AddedOrRemovedBrokerParameters, Function<String, OperationFuture>> function;
    if (endPoint == ADD_BROKER) {
      function = parameters -> (uuid -> _asyncKafkaCruiseControl.addBrokers(parameters, uuid));
    } else {
      function = parameters -> (uuid -> _asyncKafkaCruiseControl.decommissionBrokers(parameters, uuid));
    }

    asyncRequest(paramSupplier, function, request, response);
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

  private <P extends CruiseControlParameters, R extends CruiseControlResponse> void syncRequest(Supplier<P> paramSupplier,
                                                                                                Function<P, R> resultFunction,
                                                                                                HttpServletRequest request,
                                                                                                HttpServletResponse response,
                                                                                                EndPoint endPoint)
      throws ExecutionException, InterruptedException, IOException {
    long requestExecutionStartTime = System.nanoTime();
    P parameters = paramSupplier.get();

    if (!parameters.parseParameters(response)) {
      // Successfully parsed parameters.
      int step = 0;

      OperationFuture resultFuture = _userTaskManager.getOrCreateUserTask(request, response, uuid -> {
        OperationFuture future = new OperationFuture(String.format("%s request", parameters.endPoint().toString()));
        future.complete(resultFunction.apply(parameters));
        return future;
      }, step, false).get(step);

      CruiseControlResponse result = resultFuture.get();

      result.writeSuccessResponse(parameters, response);
      _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
    } else {
      LOG.warn("Failed to parse parameters: {} for sync request: {}.", request.getParameterMap(), request.getPathInfo());
    }
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
