/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#REVIEW_BOARD}.
 *
 * <pre>
 *    GET /kafkacruisecontrol/review_board?json=[true/false]&amp;review_ids=[id1,id2,...]
 * </pre>
 */
public class ReviewBoardParameters extends AbstractParameters {
  private Set<Integer> _reviewIds;

  public ReviewBoardParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _reviewIds = ParameterUtils.reviewIds(_request);
  }

  public Set<Integer> reviewIds() {
    return _reviewIds;
  }
}
