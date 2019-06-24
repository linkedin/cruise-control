/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewParameters;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;


public class ReviewRequest extends AbstractSyncRequest {
  private final Purgatory _purgatory;
  private final ReviewParameters _parameters;

  public ReviewRequest(KafkaCruiseControlServlet servlet, ReviewParameters parameters) {
    super(servlet);
    _purgatory = servlet.purgatory();
    _parameters = parameters;
  }

  @Override
  protected ReviewResult handle() {
    return _purgatory.applyReview(_parameters.reviewRequests(), _parameters.reason());
  }

  @Override
  public ReviewParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ReviewRequest.class.getSimpleName();
  }
}
