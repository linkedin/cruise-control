/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#KAFKA_CLUSTER_STATE}
 *
 * <pre>
 * Retrieve the kafka cluster state.
 *    GET /kafkacruisecontrol/kafka_cluster_state?verbose=[true/false]&amp;json=[true/false]
 * </pre>
 */
public class KafkaClusterStateParameters extends AbstractParameters {
  private boolean _isVerbose;

  public KafkaClusterStateParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _isVerbose = ParameterUtils.isVerbose(_request);
  }

  public boolean isVerbose() {
    return _isVerbose;
  }
}
