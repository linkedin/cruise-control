/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;


/**
 * Parameters for {@link CruiseControlEndPoint#PROPOSALS}
 *
 * <pre>
 *    GET /kafkacruisecontrol/proposals?verbose=[ENABLE_VERBOSE]&amp;ignore_proposal_cache=[true/false]
 *    &amp;goals=[goal1,goal2...]&amp;data_from=[valid_windows/valid_partitions]&amp;excluded_topics=[pattern]
 *    &amp;use_ready_default_goals=[true/false]&amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;exclude_recently_removed_brokers=[true/false]
 *    &amp;destination_broker_ids=[id1,id2...]&amp;kafka_assigner=[true/false]
 * </pre>
 */
public class ProposalsParameters extends GoalBasedOptimizationParameters {
  private Set<Integer> _destinationBrokerIds;
  private boolean _ignoreProposalCache;

  public ProposalsParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _destinationBrokerIds = ParameterUtils.destinationBrokerIds(_request);
    _ignoreProposalCache = ParameterUtils.ignoreProposalCache(_request);
  }

  public Set<Integer> destinationBrokerIds() {
    return _destinationBrokerIds;
  }

  public boolean ignoreProposalCache() {
    return _ignoreProposalCache;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
