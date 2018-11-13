/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import javax.servlet.http.HttpServletRequest;


/**
 * A concrete class for endpoints with base parameters -- e.g.
 * {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#PAUSE_SAMPLING},
 * {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#RESUME_SAMPLING},
 * {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#STOP_PROPOSAL_EXECUTION}
 */
public class BaseParameters extends AbstractParameters {
  public BaseParameters(HttpServletRequest request) {
    super(request);
  }
}
