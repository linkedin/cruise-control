/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.RESOURCE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.START_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.END_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ENTRIES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_CAPACITY_ESTIMATION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.MAX_LOAD_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.AVG_LOAD_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TOPIC_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.MIN_VALID_PARTITION_RATIO_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_ID_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#PARTITION_LOAD}
 *
 * <pre>
 * Get the partition load sorted by the utilization of a given resource and filtered by given topic regular expression
 *    and partition number/range
 *    GET /kafkacruisecontrol/partition_load?resource=[RESOURCE]&amp;start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
 *    &amp;entries=[number-of-entries-to-show]&amp;topic=[topic]&amp;partition=[partition/start_partition-end_partition]
 *    &amp;min_valid_partition_ratio=[min_valid_partition_ratio]&amp;allow_capacity_estimation=[true/false]
 *    &amp;max_load=[true/false]&amp;avg_load=[true/false]&amp;json=[true/false]&amp;brokerid=[brokerid]
 * </pre>
 */
public class PartitionLoadParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(RESOURCE_PARAM);
    validParameterNames.add(START_MS_PARAM);
    validParameterNames.add(END_MS_PARAM);
    validParameterNames.add(ENTRIES_PARAM);
    validParameterNames.add(TOPIC_PARAM);
    validParameterNames.add(PARTITION_PARAM);
    validParameterNames.add(MIN_VALID_PARTITION_RATIO_PARAM);
    validParameterNames.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    validParameterNames.add(MAX_LOAD_PARAM);
    validParameterNames.add(AVG_LOAD_PARAM);
    validParameterNames.add(BROKER_ID_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected Resource _resource;
  protected long _startMs;
  protected long _endMs;
  protected int _entries;
  protected Pattern _topic;
  protected int _partitionUpperBoundary;
  protected int _partitionLowerBoundary;
  protected Double _minValidPartitionRatio;
  protected boolean _allowCapacityEstimation;
  protected boolean _wantMaxLoad;
  protected boolean _wantAvgLoad;
  protected Set<Integer> _brokerIds;

  public PartitionLoadParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    String resourceString = ParameterUtils.resourceString(_request);
    try {
      _resource = Resource.valueOf(resourceString.toUpperCase());
    } catch (IllegalArgumentException iae) {
      throw new UserRequestException(String.format("Invalid resource type %s. The resource type must be one of the"
                                                   + "following: CPU, DISK, NW_IN, NW_OUT", resourceString));
    }

    _wantMaxLoad = ParameterUtils.wantMaxLoad(_request);
    _wantAvgLoad = ParameterUtils.wantAvgLoad(_request);
    if (_wantMaxLoad && _wantAvgLoad) {
      throw new UserRequestException("Parameters to ask for max and avg load are mutually exclusive to each other.");
    }
    _topic = ParameterUtils.topic(_request);
    _startMs = ParameterUtils.startMs(_request);
    _endMs = ParameterUtils.endMs(_request);
    _partitionLowerBoundary = ParameterUtils.partitionBoundary(_request, false);
    _partitionUpperBoundary = ParameterUtils.partitionBoundary(_request, true);
    _entries = ParameterUtils.entries(_request);
    _minValidPartitionRatio = ParameterUtils.minValidPartitionRatio(_request);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_request);
    _brokerIds = ParameterUtils.brokerIds(_request, true);
  }

  public Resource resource() {
    return _resource;
  }

  public long startMs() {
    return _startMs;
  }

  public long endMs() {
    return _endMs;
  }

  public int entries() {
    return _entries;
  }

  public Pattern topic() {
    return _topic;
  }

  public int partitionUpperBoundary() {
    return _partitionUpperBoundary;
  }

  public int partitionLowerBoundary() {
    return _partitionLowerBoundary;
  }

  public Double minValidPartitionRatio() {
    return _minValidPartitionRatio;
  }

  public boolean allowCapacityEstimation() {
    return _allowCapacityEstimation;
  }

  public boolean wantMaxLoad() {
    return _wantMaxLoad;
  }

  public boolean wantAvgLoad() {
    return _wantAvgLoad;
  }

  public Set<Integer> brokerIds() {
    return _brokerIds;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
