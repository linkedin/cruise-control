/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#PARTITION_LOAD}
 *
 * <pre>
 * Get the partition load sorted by the utilization of a given resource and filtered by given topic regular expression
 *    and partition number/range
 *    GET /kafkacruisecontrol/partition_load?resource=[RESOURCE]&amp;start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
 *    &amp;entries=[number-of-entries-to-show]&amp;topic=[topic]&amp;partition=[partition/start_partition-end_partition]
 *    &amp;min_valid_partition_ratio=[min_valid_partition_ratio]&amp;allow_capacity_estimation=[true/false]
 *    &amp;max_load=[true/false]&amp;json=[true/false]
 * </pre>
 */
public class PartitionLoadParameters extends AbstractParameters {
  private Resource _resource;
  private long _startMs;
  private long _endMs;
  private int _entries;
  private Pattern _topic;
  private int _partitionUpperBoundary;
  private int _partitionLowerBoundary;
  private Double _minValidPartitionRatio;
  private boolean _allowCapacityEstimation;
  private boolean _wantMaxLoad;


  public PartitionLoadParameters(HttpServletRequest request) {
    super(request);
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
    _topic = ParameterUtils.topic(_request);
    Long startMsValue = ParameterUtils.startMs(_request);
    _startMs = startMsValue == null ? -1L : startMsValue;
    Long endMsValue = ParameterUtils.endMs(_request);
    _endMs = endMsValue == null ? System.currentTimeMillis() : endMsValue;
    _partitionLowerBoundary = ParameterUtils.partitionBoundary(_request, false);
    _partitionUpperBoundary = ParameterUtils.partitionBoundary(_request, true);
    _entries = ParameterUtils.entries(_request);
    _minValidPartitionRatio = ParameterUtils.minValidPartitionRatio(_request);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_request);
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
}
