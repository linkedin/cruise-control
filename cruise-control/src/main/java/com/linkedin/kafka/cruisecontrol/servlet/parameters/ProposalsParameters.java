/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;


public class ProposalsParameters extends KafkaOptimizationParameters {
  private boolean _ignoreProposalCache;
  private ParameterUtils.DataFrom _dataFrom;
  private boolean _useReadyDefaultGoals;
  private List<String> _goals;
  private Pattern _excludedTopics;

  public ProposalsParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _ignoreProposalCache = ParameterUtils.ignoreProposalCache(_request) || !_goals.isEmpty();
    _dataFrom = ParameterUtils.getDataFrom(_request);
    _useReadyDefaultGoals = ParameterUtils.useReadyDefaultGoals(_request);
    _goals = ParameterUtils.getGoals(_request);
    _excludedTopics = ParameterUtils.excludedTopics(_request);
  }

  public boolean ignoreProposalCache() {
    return _ignoreProposalCache;
  }

  public ParameterUtils.DataFrom dataFrom() {
    return _dataFrom;
  }

  public boolean useReadyDefaultGoals() {
    return _useReadyDefaultGoals;
  }

  public List<String> goals() {
    return _goals;
  }

  public Pattern excludedTopics() {
    return _excludedTopics;
  }
}
