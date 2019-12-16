/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.areAllParametersNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_ID_PARAM;
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
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;reason=[reason-for-request]
 *    &amp;stop_ongoing_execution=[true/false]
 * </pre>
 */
public class TopicConfigurationParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(REVIEW_ID_PARAM);
    validParameterNames.addAll(TopicReplicationFactorChangeParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
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
    if (areAllParametersNull(_topicReplicationFactorChangeParameters)) {
      throw new UserRequestException("Nothing executable found in request.");
    }
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

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}