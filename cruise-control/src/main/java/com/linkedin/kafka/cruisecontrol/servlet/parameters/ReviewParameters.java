/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;


/**
 * Parameters for {@link CruiseControlEndPoint#REVIEW}.
 *
 * <pre>
 *    POST /kafkacruisecontrol/review?json=[true/false]&amp;approve=[id1,id2,...]&amp;discard=[id1,id2,...]
 *    &amp;reason=[reason-for-review]
 * </pre>
 */
public class ReviewParameters extends AbstractParameters {
  protected String _reason;
  protected Map<ReviewStatus, Set<Integer>> _reviewRequests;

  public ReviewParameters() {
    super();
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

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
