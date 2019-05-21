/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#TOPIC_CONFIGURATION}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/topic_configuration?json=[true/false]&amp;topic=[topic]
 *    &amp;replication_factor=[target_replication_factor]&amp;skip_rack_awareness_check=[true/false]
 *    &amp;review_id=[id]
 * </pre>
 */
public class TopicConfigurationParameters extends AbstractParameters {
  private Pattern _topic;
  private short _replicationFactor;
  private boolean _skipRackAwarenessCheck;
  private Integer _reviewId;

  public TopicConfigurationParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _topic = ParameterUtils.topic(_request);
    if (_topic == null) {
      throw new IllegalArgumentException("Topic to update configuration is not specified.");
    }
    _replicationFactor = ParameterUtils.replicationFactor(_request);
    if (_replicationFactor < 1) {
      throw new IllegalArgumentException("Target replication factor cannot be set to smaller than 1.");
    }
    _skipRackAwarenessCheck = ParameterUtils.skipRackAwarenessCheck(_request);
    boolean twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_request, twoStepVerificationEnabled);
  }

  public Pattern topic() {
    return _topic;
  }

  public short replicationFactor() {
    return _replicationFactor;
  }

  public boolean skipRackAwarenessCheck() {
    return _skipRackAwarenessCheck;
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }
}