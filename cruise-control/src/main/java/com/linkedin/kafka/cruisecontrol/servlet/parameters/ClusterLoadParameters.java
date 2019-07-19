/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.UnsupportedEncodingException;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#LOAD}
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
  private long _endMs;
  private long _startMs;
  private ModelCompletenessRequirements _requirements;
  private boolean _allowCapacityEstimation;
  private boolean _populateDiskInfo;

  public ClusterLoadParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
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
}
