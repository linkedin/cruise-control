/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewBoardParameters;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_BOARD_PARAMETER_OBJECT_CONFIG;


public class ReviewBoardRequest extends AbstractSyncRequest {
  private Purgatory _purgatory;
  private ReviewBoardParameters _parameters;

  public ReviewBoardRequest() {
    super();
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

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _purgatory = _servlet.purgatory();
    _parameters = (ReviewBoardParameters) configs.get(REVIEW_BOARD_PARAMETER_OBJECT_CONFIG);
    if (_parameters == null) {
      throw new IllegalArgumentException("Parameter configuration is missing from the request.");
    }
  }
}
