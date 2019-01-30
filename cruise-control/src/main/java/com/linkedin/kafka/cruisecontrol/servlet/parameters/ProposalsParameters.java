/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#PROPOSALS}
 *
 * <pre>
 *    GET /kafkacruisecontrol/proposals?verbose=[ENABLE_VERBOSE]&amp;ignore_proposal_cache=[true/false]
 *    &amp;goals=[goal1,goal2...]&amp;data_from=[valid_windows/valid_partitions]&amp;excluded_topics=[pattern]
 *    &amp;use_ready_default_goals=[true/false]&amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;exclude_recently_removed_brokers=[true/false]
 * </pre>
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
