/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;


/**
 * Parameters for {@link CruiseControlEndPoint#REVIEW_BOARD}.
 *
 * <pre>
 *    GET /kafkacruisecontrol/review_board?json=[true/false]&amp;review_ids=[id1,id2,...]
 * </pre>
 */
public class ReviewBoardParameters extends AbstractParameters {
  private Set<Integer> _reviewIds;

  public ReviewBoardParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _reviewIds = ParameterUtils.reviewIds(_request);
  }

  public Set<Integer> reviewIds() {
    return _reviewIds;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
