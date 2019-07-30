/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;


/**
 * Parameters for {@link CruiseControlEndPoint#ADD_BROKER}
 *<ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *</ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]
 *    &amp;throttle_added_broker=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
 *    &amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]&amp;concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;kafka_assigner=[true/false]
 *    &amp;use_ready_default_goals=[true/false]&amp;verbose=[true/false]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;review_id=[id]
 * </pre>
 */
public class AddBrokerParameters extends AddedOrRemovedBrokerParameters {
  protected boolean _throttleAddedBrokers;

  public AddBrokerParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _throttleAddedBrokers = ParameterUtils.throttleAddedOrRemovedBrokers(_request, _endPoint);
  }

  public boolean throttleAddedBrokers() {
    return _throttleAddedBrokers;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }
}
