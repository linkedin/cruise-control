/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#REVIEW}.
 *
 * <pre>
 *    POST /kafkacruisecontrol/review?json=[true/false]&amp;approve=[id1,id2,...]&amp;discard=[id1,id2,...]
 *    &amp;reason=[reason-for-review]
 * </pre>
 */
public class ReviewParameters extends AbstractParameters {
  private String _reason;
  private Map<ReviewStatus, Set<Integer>> _reviewRequests;

  public ReviewParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _reason = ParameterUtils.reason(_request);
    _reviewRequests = ParameterUtils.reviewRequests(_request);
  }

  public String reason() {
    return _reason;
  }

  public Map<ReviewStatus, Set<Integer>> reviewRequests() {
    return _reviewRequests;
  }
}
