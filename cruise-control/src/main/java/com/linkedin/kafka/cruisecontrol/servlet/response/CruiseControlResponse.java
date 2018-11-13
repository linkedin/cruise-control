/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;


public interface CruiseControlResponse {

  /**
   * Write success response with the given parameters to the provided HTTP response.
   *
   * @param parameters Parameters of the HTTP request of user.
   * @param response HTTP response to return to user.
   */
  void writeSuccessResponse(CruiseControlParameters parameters, HttpServletResponse response) throws IOException;
}
