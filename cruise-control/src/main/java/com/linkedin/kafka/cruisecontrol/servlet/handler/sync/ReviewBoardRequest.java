/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewBoardParameters;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;


public class ReviewBoardRequest extends AbstractSyncRequest {
  private final Purgatory _purgatory;
  private final ReviewBoardParameters _parameters;

  public ReviewBoardRequest(KafkaCruiseControlServlet servlet, ReviewBoardParameters parameters) {
    super(servlet);
    _purgatory = servlet.purgatory();
    _parameters = parameters;
  }

  @Override
  protected ReviewResult handle() {
    return _purgatory.reviewBoard(_parameters.reviewIds());
  }

  @Override
  public ReviewBoardParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ReviewBoardRequest.class.getSimpleName();
  }
}
