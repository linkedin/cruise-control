/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.sanityCheckOptionalParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicReplicationFactorChangeParameters.maybeBuildTopicReplicationFactorChangeParameters;


/**
 * Parameters for {@link CruiseControlEndPoint#TOPIC_CONFIGURATION}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *   <li>If topics to change replication factor and target replication factor is not specified in URL, they can also be
 *   specified in request body. The body format is expected to be valid JSON format. e.g.
 *    <pre><code>
 *   {
 *       replication_factor: {
 *           topic_by_replication_factor : {
 *               target_replication_factor_1 : topic_regex_1,
 *               target_replication_factor_2 : topic_regex_2,
 *               ...
 *       }
 *   }
 *   </code></pre>
 *   If user specifies new replication factor in both URL (via combination of `topic` and `replication_factor` parameter)
 *   and body, an exception will be thrown.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/topic_configuration?json=[true/false]&amp;verbose=[true/false]&amp;topic=[topic]
 *    &amp;replication_factor=[target_replication_factor]&amp;skip_rack_awareness_check=[true/false]
 *    &amp;dryRun=[true/false]&amp;goals=[goal1,goal2...]&amp;skip_hard_goal_check=[true/false]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;review_id=[id]&amp;replication_throttle=[bytes_per_second]
 * </pre>
 */
public class TopicConfigurationParameters extends GoalBasedOptimizationParameters {
  protected Integer _reviewId;
  protected TopicReplicationFactorChangeParameters _topicReplicationFactorChangeParameters;
  protected Map<String, ?> _configs;

  public TopicConfigurationParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    boolean twoStepVerificationEnabled = _config.getBoolean(KafkaCruiseControlConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_request, twoStepVerificationEnabled);
    _topicReplicationFactorChangeParameters = maybeBuildTopicReplicationFactorChangeParameters(_configs);
    sanityCheckOptionalParameters(_topicReplicationFactorChangeParameters);
  }


  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _configs = configs;
  }

  public TopicReplicationFactorChangeParameters topicReplicationFactorChangeParameters() {
    return _topicReplicationFactorChangeParameters;
  }

  /**
   * Supported topic configuration type to be changed via {@link CruiseControlEndPoint#TOPIC_CONFIGURATION} endpoint.
   */
  public enum TopicConfigurationType {
    REPLICATION_FACTOR
  }
}