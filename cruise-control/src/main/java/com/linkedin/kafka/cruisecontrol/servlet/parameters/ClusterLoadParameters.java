/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TIME_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.END_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.START_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_CAPACITY_ESTIMATION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.POPULATE_DISK_INFO_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#LOAD}
 *
 * <ul>
 *   <li>Note that both parameter "time" and "end" are used to specify the end time for cluster model, thus they are mutually exclusive.</li>
 *</ul>
 *
 * <pre>
 * Get the cluster load
 *    GET /kafkacruisecontrol/load?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]&amp;time=[END_TIMESTAMP]&amp;allow_capacity_estimation=[true/false]
 *    &amp;json=[true/false]&amp;populate_disk_info=[true/false]
 * </pre>
 */
public class ClusterLoadParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(TIME_PARAM);
    validParameterNames.add(END_MS_PARAM);
    validParameterNames.add(START_MS_PARAM);
    validParameterNames.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    validParameterNames.add(POPULATE_DISK_INFO_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected long _endMs;
  protected long _startMs;
  protected ModelCompletenessRequirements _requirements;
  protected boolean _allowCapacityEstimation;
  protected boolean _populateDiskInfo;

  public ClusterLoadParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    Long time = ParameterUtils.time(_request);
    _endMs = time == null ? ParameterUtils.endMs(_request) : time;
    _startMs = ParameterUtils.startMs(_request);
    _requirements = new ModelCompletenessRequirements(1, 0.0, true);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_request);
    _populateDiskInfo = ParameterUtils.populateDiskInfo(_request);
  }

  public long startMs() {
    return _startMs;
  }

  public long endMs() {
    return _endMs;
  }

  public ModelCompletenessRequirements requirements() {
    return _requirements;
  }

  public boolean allowCapacityEstimation() {
    return _allowCapacityEstimation;
  }

  public boolean populateDiskInfo() {
    return _populateDiskInfo;
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
