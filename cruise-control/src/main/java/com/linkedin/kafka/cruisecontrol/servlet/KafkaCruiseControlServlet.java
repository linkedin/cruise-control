/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.handler.Request;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.RequestParameterWrapper;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.KAFKA_CRUISE_CONTROL_SERVLET_SENSOR;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REVIEW;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REVIEW_BOARD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.hasValidParameterNames;


/**
 * The servlet for Kafka Cruise Control.
 */
public class KafkaCruiseControlServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlServlet.class);
  private final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
  private final KafkaCruiseControlConfig _config;
  private final UserTaskManager _userTaskManager;
  private final ThreadLocal<Integer> _asyncOperationStep;
  private final Map<EndPoint, Meter> _requestMeter = new HashMap<>();
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer = new HashMap<>();
  private final boolean _twoStepVerification;
  private final Purgatory _purgatory;

  public KafkaCruiseControlServlet(AsyncKafkaCruiseControl asynckafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
    _config = asynckafkaCruiseControl.config();
    _asyncKafkaCruiseControl = asynckafkaCruiseControl;
    _twoStepVerification = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _purgatory = _twoStepVerification ? new Purgatory(_config) : null;
    _userTaskManager = new UserTaskManager(_config, dropwizardMetricRegistry, _successfulRequestExecutionTimer, _purgatory);
    _asyncKafkaCruiseControl.setUserTaskManagerInExecutor(_userTaskManager);
    _asyncOperationStep = new ThreadLocal<>();
    _asyncOperationStep.set(0);

    for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.cachedValues()) {
      _requestMeter.put(endpoint, dropwizardMetricRegistry.meter(
          MetricRegistry.name(KAFKA_CRUISE_CONTROL_SERVLET_SENSOR, endpoint.name() + "-request-rate")));
      _successfulRequestExecutionTimer.put(endpoint, dropwizardMetricRegistry.timer(
          MetricRegistry.name(KAFKA_CRUISE_CONTROL_SERVLET_SENSOR, endpoint.name() + "-successful-request-execution-timer")));
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    _userTaskManager.close();
    if (_purgatory != null) {
      _purgatory.close();
    }
  }

  public AsyncKafkaCruiseControl asyncKafkaCruiseControl() {
    return _asyncKafkaCruiseControl;
  }

  public Map<EndPoint, Timer> successfulRequestExecutionTimer() {
    return Collections.unmodifiableMap(_successfulRequestExecutionTimer);
  }

  public ThreadLocal<Integer> asyncOperationStep() {
    return _asyncOperationStep;
  }

  public UserTaskManager userTaskManager() {
    return _userTaskManager;
  }

  protected void doOptions(HttpServletRequest request, HttpServletResponse response) {
    handleOptions(response, _config);
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
      CruiseControlEndPoint endPoint = getValidEndpoint(request, response, _config);
      if (endPoint != null) {
        _requestMeter.get(endPoint).mark();
        Map<String, Object> requestConfigOverrides = new HashMap<>();
        requestConfigOverrides.put(KAFKA_CRUISE_CONTROL_SERVLET_OBJECT_CONFIG, this);

        Map<String, Object> parameterConfigOverrides = new HashMap<>();
        parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG, request);
        parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG, _config);

        switch (request.getMethod()) {
          case GET_METHOD:
            handleGet(request, response, endPoint, requestConfigOverrides, parameterConfigOverrides);
            break;
          case POST_METHOD:
            handlePost(request, response, endPoint, requestConfigOverrides, parameterConfigOverrides);
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
   * The GET method allows users to perform actions supported by {@link CruiseControlEndPoint#getEndpoints()}.
   * @param request HTTP request received by Cruise Control.
   * @param response HTTP response of Cruise Control.
   * @param endPoint A GET endpoint of Cruise Control.
   * @param requestConfigOverrides Config overrides to be used while creating the {@link Request}.
   * @param parameterConfigOverrides Config overrides to be used while creating the {@link CruiseControlParameters}.
   */
  private void handleGet(HttpServletRequest request,
                         HttpServletResponse response,
                         CruiseControlEndPoint endPoint,
                         Map<String, Object> requestConfigOverrides,
                         Map<String, Object> parameterConfigOverrides)
      throws InterruptedException, ExecutionException, IOException {
    // Sanity check: if the request is for REVIEW_BOARD, two step verification must be enabled.
    if (endPoint == REVIEW_BOARD && !_twoStepVerification) {
      throw new ConfigException(String.format("Attempt to access %s endpoint without enabling '%s' config.",
                                              endPoint, WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG));
    }

    RequestParameterWrapper requestParameter = requestParameterFor(endPoint);
    CruiseControlParameters parameters = _config.getConfiguredInstance(requestParameter.parametersClass(),
                                                                       CruiseControlParameters.class,
                                                                       parameterConfigOverrides);
    if (hasValidParameterNames(request, response, _config, parameters)) {
      requestConfigOverrides.put(requestParameter.parameterObject(), parameters);
      Request ccRequest = _config.getConfiguredInstance(requestParameter.requestClass(), Request.class, requestConfigOverrides);

      ccRequest.handle(request, response);
    }
  }

  /**
   * The POST method allows users to perform actions supported by {@link CruiseControlEndPoint#postEndpoints()}.
   * @param request HTTP request received by Cruise Control.
   * @param response HTTP response of Cruise Control.
   * @param endPoint A POST endpoint of Cruise Control.
   * @param requestConfigOverrides Config overrides to be used while creating the {@link Request}.
   * @param parameterConfigOverrides Config overrides to be used while creating the {@link CruiseControlParameters}.
   */
  private void handlePost(HttpServletRequest request,
                          HttpServletResponse response,
                          CruiseControlEndPoint endPoint,
                          Map<String, Object> requestConfigOverrides,
                          Map<String, Object> parameterConfigOverrides)
      throws InterruptedException, ExecutionException, IOException {
    CruiseControlParameters parameters;
    RequestParameterWrapper requestParameter = requestParameterFor(endPoint);
    if (endPoint == REVIEW) {
      // Sanity check: if the request is for REVIEW, two step verification must be enabled.
      if (!_twoStepVerification) {
        throw new ConfigException(String.format("Attempt to access %s endpoint without enabling '%s' config.",
                                                endPoint, WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG));
      }

      parameters = _config.getConfiguredInstance(requestParameter.parametersClass(), CruiseControlParameters.class, parameterConfigOverrides);
      if (!hasValidParameterNames(request, response, _config, parameters)) {
        return;
      }
    } else if (!_twoStepVerification) {
      // Do not add to the purgatory if the two-step verification is disabled.
      parameters = _config.getConfiguredInstance(requestParameter.parametersClass(), CruiseControlParameters.class, parameterConfigOverrides);
      if (!hasValidParameterNames(request, response, _config, parameters)) {
        return;
      }
    } else {
      // Add to the purgatory if the two-step verification is enabled.
      parameters = _purgatory.maybeAddToPurgatory(request, response, requestParameter.parametersClass(), parameterConfigOverrides, _userTaskManager);
    }

    Request ccRequest = null;
    if (parameters != null) {
      requestConfigOverrides.put(requestParameter.parameterObject(), parameters);
      ccRequest = _config.getConfiguredInstance(requestParameter.requestClass(), Request.class, requestConfigOverrides);
    }

    if (ccRequest != null) {
      // ccRequest would be null if request is added to Purgatory.
      ccRequest.handle(request, response);
    }
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
