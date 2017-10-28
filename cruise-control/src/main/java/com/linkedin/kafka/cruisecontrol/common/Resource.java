/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * CPU: a host and broker-level resource.
 * NW (in and out): a host-level resource.
 * DISK: a broker-level resource.
 */
public enum Resource {
  CPU("cpu", 0, true, true, 0.001),
  NW_IN("networkInbound", 1, true, false, 10),
  NW_OUT("networkOutbound", 2, true, false, 10),
  DISK("disk", 3, false, true, 100);

  private static final double EPSILON_PERCENT = 1E-3;
  private final String _resource;
  private final int _id;
  private final boolean _isHostResource;
  private final boolean _isBrokerResource;
  private double _epsilon;

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

  public String resource() {
    return _resource;
  }

  public int id() {
    return _id;
  }

  public boolean isHostResource() {
    return _isHostResource;
  }

  public boolean isBrokerResource() {
    return _isBrokerResource;
  }

  public double epsilon(double value1, double value2) {
    return Math.max(_epsilon, EPSILON_PERCENT * (value1 + value2) / 2);
  }

  @Override
  public String toString() {
    return _resource;
  }
}
