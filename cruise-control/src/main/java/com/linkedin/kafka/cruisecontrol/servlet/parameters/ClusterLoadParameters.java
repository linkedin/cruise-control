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
 * <pre>
 * Get the cluster load
 *    GET /kafkacruisecontrol/load?time=[TIMESTAMP]&amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]
 *    &amp;populate_disk_info=[true/false]
 * </pre>
 */
public class ClusterLoadParameters extends AbstractParameters {
  private long _time;
  private ModelCompletenessRequirements _requirements;
  private boolean _allowCapacityEstimation;
  private boolean _populateDiskInfo;

  public ClusterLoadParameters(HttpServletRequest request, KafkaCruiseControlConfig config) {
    super(request, config);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _time = ParameterUtils.time(_request);
    _requirements = new ModelCompletenessRequirements(1, 0.0, true);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_request);
    _populateDiskInfo = ParameterUtils.populateDiskInfo(_request);
  }

  public long time() {
    return _time;
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
