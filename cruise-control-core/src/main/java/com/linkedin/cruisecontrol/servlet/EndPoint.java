/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.servlet;

/**
 * An enum that lists all supported endpoints by Cruise Control.
 */
public interface EndPoint {

  /**
   * @return The endpoint type, which might be relevant for the application logic -- e.g. selective caching the response,
   * specifying custom retention for endpoints with certain type.
   */
  EndpointType endpointType();
}
