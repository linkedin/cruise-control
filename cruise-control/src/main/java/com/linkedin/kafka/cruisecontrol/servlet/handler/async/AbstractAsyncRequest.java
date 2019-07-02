/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.handler.AbstractRequest;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.response.ProgressResult;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractAsyncRequest extends AbstractRequest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAsyncRequest.class);
  protected final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
  private final ThreadLocal<Integer> _asyncOperationStep;
  private final UserTaskManager _userTaskManager;
  private final long _maxBlockMs;

  public AbstractAsyncRequest(KafkaCruiseControlServlet servlet) {
    _asyncKafkaCruiseControl = servlet.asyncKafkaCruiseControl();
    _asyncOperationStep = servlet.asyncOperationStep();
    _userTaskManager = servlet.userTaskManager();
    _maxBlockMs = servlet.maxBlockMs();
  }

  /**
   * Handle the request with the given uuid and return the corresponding {@link OperationFuture}.
   *
   * @param uuid UUID string associated with the request.
   * @return the corresponding {@link OperationFuture}.
   */
  protected abstract OperationFuture handle(String uuid);

  @Override
  public CruiseControlResponse getResponse(HttpServletRequest request, HttpServletResponse response)
      throws ExecutionException, InterruptedException {
    LOG.info("Processing async request {}.", name());
    int step = _asyncOperationStep.get();
    List<OperationFuture>
        futures = _userTaskManager.getOrCreateUserTask(request, response, this::handle, step, true, parameters());
    _asyncOperationStep.set(step + 1);
    CruiseControlResponse ccResponse;
    try {
      ccResponse = futures.get(step).get(_maxBlockMs, TimeUnit.MILLISECONDS);
      LOG.info("Computation is completed for async request: {}.", request.getPathInfo());
    } catch (TimeoutException te) {
      ccResponse = new ProgressResult(futures, _asyncKafkaCruiseControl.config());
      LOG.info("Computation is in progress for async request: {}.", request.getPathInfo());
    }
    return ccResponse;
  }

  @Override
  public abstract CruiseControlParameters parameters();

  public abstract String name();
}
