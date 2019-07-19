/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#REMOVE_BROKER}
 *<ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *</ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/remove_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]
 *    &amp;throttle_removed_broker=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
 *    &amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]&amp;concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;kafka_assigner=[true/false]
 *    &amp;use_ready_default_goals=[true/false]&amp;verbose=[true/false]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;destination_broker_ids=[id1,id2...]&amp;review_id=[id]&amp;replication_throttle=[bytes_per_second]
 * </pre>
 */
public class RemoveBrokerParameters extends AddedOrRemovedBrokerParameters {
  private boolean _throttleRemovedBrokers;
  private Set<Integer> _destinationBrokerIds;

  public RemoveBrokerParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _throttleRemovedBrokers = ParameterUtils.throttleAddedOrRemovedBrokers(_request, _endPoint);
    _destinationBrokerIds = ParameterUtils.destinationBrokerIds(_request);
  }

  public boolean throttleRemovedBrokers() {
    return _throttleRemovedBrokers;
  }

  public Set<Integer> destinationBrokerIds() {
    return _destinationBrokerIds;
  }
}


