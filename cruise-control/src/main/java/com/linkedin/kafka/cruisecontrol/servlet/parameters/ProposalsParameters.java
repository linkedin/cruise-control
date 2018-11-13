/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#PROPOSALS}
 */
public class ProposalsParameters extends GoalBasedOptimizationParameters {
  private boolean _ignoreProposalCache;

  public ProposalsParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _ignoreProposalCache = ParameterUtils.ignoreProposalCache(_request) || !_goals.isEmpty();
  }

  public boolean ignoreProposalCache() {
    return _ignoreProposalCache;
  }
}
