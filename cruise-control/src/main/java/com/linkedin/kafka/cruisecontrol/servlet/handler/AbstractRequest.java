/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler;

import com.linkedin.cruisecontrol.servlet.handler.Request;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_SERVLET_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public abstract class AbstractRequest implements Request {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRequest.class);
  protected KafkaCruiseControlServlet _servlet;

  /**
   * Handle the request and populate the response.
   *
   * @param request Http servlet request.
   * @param response Http servlet response.
   */
  @Override
  public void handle(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException {
    if (parameters().parseParameters(response)) {
      LOG.warn("Failed to parse parameters: {} for request: {}.", request.getParameterMap(), request.getPathInfo());
      return;
    }

    CruiseControlResponse ccResponse = getResponse(request, response);
    ccResponse.writeSuccessResponse(parameters(), response);
  }

  /**
   * Get the response of the request
   * <ul>
   *   <li>Asynchronous requests return either the final response or the progress of the async request.</li>
   *   <li>Synchronous requests return the final response of the sync request.</li>
   * </ul>
   *
   * @param request Http servlet request.
   * @param response Http servlet response.
   * @return Response of the requests.
   */
  protected abstract CruiseControlResponse getResponse(HttpServletRequest request, HttpServletResponse response)
      throws ExecutionException, InterruptedException;

  public abstract CruiseControlParameters parameters();

  @Override
  public void configure(Map<String, ?> configs) {
    _servlet = (KafkaCruiseControlServlet) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_SERVLET_OBJECT_CONFIG),
            "Kafka Cruise Control servlet configuration is missing from the request.");
  }
}
