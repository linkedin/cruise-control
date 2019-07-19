/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.Request;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.AddBrokerRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.ClusterLoadRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.CruiseControlStateRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.DemoteRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.FixOfflineReplicasRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.PartitionLoadRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.ProposalsRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.RebalanceRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.RemoveBrokerRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.TopicConfigurationRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.AdminRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.BootstrapRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.KafkaClusterStateRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.PauseRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ResumeRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ReviewBoardRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ReviewRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.StopProposalRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.TrainRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.UserTasksRequest;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AdminParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewBoardParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.RequestInfo;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.hasValidParameters;
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
    _asyncKafkaCruiseControl.setUserTaskManagerInExecutor(_userTaskManager);
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

  public AsyncKafkaCruiseControl asyncKafkaCruiseControl() {
    return _asyncKafkaCruiseControl;
  }

  public Map<EndPoint, Timer> successfulRequestExecutionTimer() {
    return _successfulRequestExecutionTimer;
  }

  public ThreadLocal<Integer> asyncOperationStep() {
    return _asyncOperationStep;
  }

  public UserTaskManager userTaskManager() {
    return _userTaskManager;
  }

  public long maxBlockMs() {
    return _maxBlockMs;
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

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    doGetOrPost(request, response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    doGetOrPost(request, response);
  }

  private void doGetOrPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = getValidEndpoint(request, response, _config);
      if (endPoint != null && hasValidParameters(request, response, _config)) {
        _requestMeter.get(endPoint).mark();
        switch (request.getMethod()) {
          case GET_METHOD:
            handleGet(request, response, endPoint);
            break;
          case POST_METHOD:
            handlePost(request, response, endPoint);
            break;
          default:
            throw new IllegalArgumentException("Unsupported request method: " + request.getMethod() + ".");
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
   * The GET method allows users to perform the following actions:
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
  private void handleGet(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws InterruptedException, ExecutionException, IOException {
    Request ccRequest;
    switch (endPoint) {
      case BOOTSTRAP:
        ccRequest = new BootstrapRequest(this, new BootstrapParameters(request, _config));
        break;
      case TRAIN:
        ccRequest = new TrainRequest(this, new TrainParameters(request, _config));
        break;
      case LOAD:
        ccRequest = new ClusterLoadRequest(this, new ClusterLoadParameters(request, _config));
        break;
      case PARTITION_LOAD:
        ccRequest = new PartitionLoadRequest(this, new PartitionLoadParameters(request, _config));
        break;
      case PROPOSALS:
        ccRequest = new ProposalsRequest(this, new ProposalsParameters(request, _config));
        break;
      case STATE:
        ccRequest = new CruiseControlStateRequest(this, new CruiseControlStateParameters(request, _config));
        break;
      case KAFKA_CLUSTER_STATE:
        ccRequest = new KafkaClusterStateRequest(this, new KafkaClusterStateParameters(request, _config));
        break;
      case USER_TASKS:
        ccRequest = new UserTasksRequest(this, new UserTasksParameters(request, _config));
        break;
      case REVIEW_BOARD:
        if (!_twoStepVerification) {
          throw new ConfigException(String.format("Attempt to access %s endpoint without enabling '%s' config.",
                                                  endPoint, KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG));
        }
        ccRequest = new ReviewBoardRequest(this, new ReviewBoardParameters(request, _config));
        break;
      default:
        throw new UserRequestException("Invalid URL for GET");
    }

    ccRequest.handle(request, response);
  }

  /**
   * The POST method allows users to perform the following actions:
   *
   * <pre>
   * 1. Decommission a broker (See {@link RemoveBrokerParameters}).
   * 2. Add a broker (See {@link AddBrokerParameters}).
   * 3. Trigger a workload balance (See {@link RebalanceParameters}).
   * 4. Stop the proposal execution (See {@link StopProposalParameters}).
   * 5. Pause metrics sampling (See {@link PauseResumeParameters}).
   * 6. Resume metrics sampling (See {@link PauseResumeParameters}).
   * 7. Demote a broker (See {@link DemoteBrokerParameters}).
   * 8. Admin operations on Cruise Control (See {@link AdminParameters}).
   * 9. Change topic configurations (See {@link TopicConfigurationParameters}).
   * 10. Review requests for two-step verification (See {@link ReviewParameters}).
   * 11. Fix offline replicas (See {@link FixOfflineReplicasParameters}).
   * </pre>
   */
  private void handlePost(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws InterruptedException, ExecutionException, IOException {
    CruiseControlParameters reviewableParams;
    Request ccRequest = null;
    switch (endPoint) {
      case ADD_BROKER:
        reviewableParams = evaluateReviewableParams(request, response, () -> new AddBrokerParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new AddBrokerRequest(this, (AddBrokerParameters) reviewableParams);
        }
        break;
      case REMOVE_BROKER:
        reviewableParams = evaluateReviewableParams(request, response, () -> new RemoveBrokerParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new RemoveBrokerRequest(this, (RemoveBrokerParameters) reviewableParams);
        }
        break;
      case FIX_OFFLINE_REPLICAS:
        reviewableParams = evaluateReviewableParams(request, response, () -> new FixOfflineReplicasParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new FixOfflineReplicasRequest(this, (FixOfflineReplicasParameters) reviewableParams);
        }
        break;
      case REBALANCE:
        reviewableParams = evaluateReviewableParams(request, response, () -> new RebalanceParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new RebalanceRequest(this, (RebalanceParameters) reviewableParams);
        }
        break;
      case STOP_PROPOSAL_EXECUTION:
        reviewableParams = evaluateReviewableParams(request, response, () -> new StopProposalParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new StopProposalRequest(this, (StopProposalParameters) reviewableParams);
        }
        break;
      case PAUSE_SAMPLING:
        reviewableParams = evaluateReviewableParams(request, response, () -> new PauseResumeParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new PauseRequest(this, (PauseResumeParameters) reviewableParams);
        }
        break;
      case RESUME_SAMPLING:
        reviewableParams = evaluateReviewableParams(request, response, () -> new PauseResumeParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new ResumeRequest(this, (PauseResumeParameters) reviewableParams);
        }
        break;
      case DEMOTE_BROKER:
        reviewableParams = evaluateReviewableParams(request, response, () -> new DemoteBrokerParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new DemoteRequest(this, (DemoteBrokerParameters) reviewableParams);
        }
        break;
      case ADMIN:
        reviewableParams = evaluateReviewableParams(request, response, () -> new AdminParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new AdminRequest(this, (AdminParameters) reviewableParams);
        }
        break;
      case TOPIC_CONFIGURATION:
        reviewableParams = evaluateReviewableParams(request, response, () -> new TopicConfigurationParameters(request, _config));
        if (reviewableParams != null) {
          ccRequest = new TopicConfigurationRequest(this, (TopicConfigurationParameters) reviewableParams);
        }
        break;
      case REVIEW:
        if (!_twoStepVerification) {
          throw new ConfigException(String.format("Attempt to access %s endpoint without enabling '%s' config.",
                                                  endPoint, KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG));
        }
        ccRequest = new ReviewRequest(this, new ReviewParameters(request, _config));
        break;
      default:
        throw new UserRequestException("Invalid URL for POST");
    }

    if (ccRequest != null) {
      // ccRequest would be null if request is added to Purgatory.
      ccRequest.handle(request, response);
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
   * @return All user tasks that the {@link #_userTaskManager} is aware of.
   */
  public List<UserTaskManager.UserTaskInfo> getAllUserTasks() {
    return _userTaskManager.getAllUserTasks();
  }

  /**
   * @return The servlet purgatory for requests.
   */
  public Purgatory purgatory() {
    return _purgatory;
  }
}
