/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * Parameters for {@link CruiseControlEndPoint#KAFKA_CLUSTER_STATE}
 *
 * <pre>
 * Retrieve the kafka cluster state.
 *    GET /kafkacruisecontrol/kafka_cluster_state?verbose=[true/false]&amp;json=[true/false]&amp;topic=[topic]
 * </pre>
 */
public class KafkaClusterStateParameters extends AbstractParameters {
  protected boolean _isVerbose;
  protected Pattern _topic;

  public KafkaClusterStateParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _isVerbose = ParameterUtils.isVerbose(_request);
    _topic = ParameterUtils.topic(_request);
  }

  public boolean isVerbose() {
    return _isVerbose;
  }

  public Pattern topic() {
    return _topic;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
