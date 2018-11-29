/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#TRAIN}
 *
 * <pre>
 * Train the Kafka Cruise Control linear regression model. The trained model will only be used if
 * {@link com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig#USE_LINEAR_REGRESSION_MODEL_CONFIG} is true.
 *
 *    GET /kafkacruisecontrol/train?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]&amp;json=[true/false]
 * </pre>
 */
public class TrainParameters extends AbstractParameters {
  private Long _startMs;
  private Long _endMs;

  public TrainParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _startMs = ParameterUtils.startMs(_request);
    _endMs = ParameterUtils.endMs(_request);
    if (_startMs == null || _endMs == null) {
      throw new UserRequestException("Missing start or end parameter.");
    }
  }

  public Long startMs() {
    return _startMs;
  }

  public Long endMs() {
    return _endMs;
  }
}
