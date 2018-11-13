/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import javax.servlet.http.HttpServletResponse;


public interface CruiseControlParameters {

  /**
   * @param response HTTP response of Cruise Control.
   * @return True if there has been a failure to parse parameters, false otherwise.
   */
  boolean parseParameters(HttpServletResponse response);

  /**
   * @return Endpoint for which the parameters are parsed.
   */
  EndPoint endPoint();

  /**
   * @return True if requested response is in JSON, false otherwise.
   */
  boolean json();
}
