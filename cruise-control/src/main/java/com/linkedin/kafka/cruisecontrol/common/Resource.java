/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * CPU: a host and broker-level resource.
 * NW (in and out): a host-level resource.
 * DISK: a broker-level resource.
 */
public enum Resource {
  @JsonResponseField
  CPU("cpu", 0, true, true, 0.001),
  @JsonResponseField
  NW_IN("networkInbound", 1, true, false, 10),
  @JsonResponseField
  NW_OUT("networkOutbound", 2, true, false, 10),
  @JsonResponseField
  DISK("disk", 3, false, true, 100);

  // EPSILON_PERCENT defines the acceptable nuance when comparing the utilization of the resource.
  // This nuance is generated due to precision loss when summing up float type utilization value.
  // In stress test we find that for cluster of around 800,000 replicas, the summed up nuance can be
  // more than 0.1% of sum value.
  private static final double EPSILON_PERCENT = 0.0008;
  private final String _resource;
  private final int _id;
  private final boolean _isHostResource;
  private final boolean _isBrokerResource;
  private final double _epsilon;

  private static final List<Resource> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<Resource> cachedValues() {
    return CACHED_VALUES;
  }

  Resource(String resource, int id, boolean isHostResource, boolean isBrokerResource, double epsilon) {
    _resource = resource;
    _id = id;
    _isHostResource = isHostResource;
    _isBrokerResource = isBrokerResource;
    _epsilon = epsilon;
  }

  /**
   * @return The resource type.
   */
  public String resource() {
    return _resource;
  }

  /**
   * @return The resource id.
   */
  public int id() {
    return _id;
  }

  /**
   * @return True if host resource, false otherwise.
   */
  public boolean isHostResource() {
    return _isHostResource;
  }

  /**
   * @return True if broker resource, false otherwise.
   */
  public boolean isBrokerResource() {
    return _isBrokerResource;
  }

  /**
   * The epsilon value used in comparing the given values.
   *
   * @param value1 The first value used in comparison.
   * @param value2 The second value used in comparison.
   * @return The epsilon value used in comparing the given values.
   */
  public double epsilon(double value1, double value2) {
    return Math.max(_epsilon, EPSILON_PERCENT * (value1 + value2));
  }

  @Override
  public String toString() {
    return _resource;
  }
}
