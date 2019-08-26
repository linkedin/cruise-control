/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;

/**
 * Optional Parameters for {@link CruiseControlEndPoint#ADMIN}.
 * This class holds all the request parameters for {@link AdminParameters.AdminType#DROP_RECENT_BROKERS}.
 */
public class DropRecentBrokersParameters extends AbstractParameters {
  protected Set<Integer> _dropRecentlyRemovedBrokers;
  protected Set<Integer> _dropRecentlyDemotedBrokers;

  protected DropRecentBrokersParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _dropRecentlyRemovedBrokers = ParameterUtils.dropRecentlyRemovedBrokers(_request);
    _dropRecentlyDemotedBrokers = ParameterUtils.dropRecentlyDemotedBrokers(_request);
  }

  /**
   * Create a {@link DropRecentBrokersParameters} object from the request.
   *
   * @param configs Information collected from request and Cruise Control configs.
   * @return a DropRecentBrokersParameters object; or null if any required parameter is not specified in the request.
   */
  static DropRecentBrokersParameters maybeBuildDropRecentBrokersParameters(Map<String, ?> configs)
      throws UnsupportedEncodingException {
    DropRecentBrokersParameters dropRecentBrokersParameters = new DropRecentBrokersParameters();
    dropRecentBrokersParameters.configure(configs);
    dropRecentBrokersParameters.initParameters();
    // At least one recently removed/demoted broker should be specified to drop; otherwise, return null.
    if (dropRecentBrokersParameters.dropRecentlyDemotedBrokers().isEmpty()
        && dropRecentBrokersParameters.dropRecentlyRemovedBrokers().isEmpty()) {
      return null;
    }
    return dropRecentBrokersParameters;
  }

  public Set<Integer> dropRecentlyRemovedBrokers() {
    return _dropRecentlyRemovedBrokers;
  }

  public Set<Integer> dropRecentlyDemotedBrokers() {
    return _dropRecentlyDemotedBrokers;
  }
}