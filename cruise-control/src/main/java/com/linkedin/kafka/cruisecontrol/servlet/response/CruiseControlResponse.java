/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;


public interface CruiseControlResponse {

  /**
   * Based on the given parameters, keep the relevant response to be returned to the user and discard the remaining.
   * Calls to this method intend to alleviate the potential memory pressure of response. Noop if called multiple times.
   *
   * @param parameters Parameters of the HTTP request of user.
   */
  void discardIrrelevantResponse(CruiseControlParameters parameters);

  /**
   * Write success response with the given parameters to the provided HTTP response.
   *
   * @param parameters Parameters of the HTTP request of user.
   * @param response HTTP response to return to user.
   */
  void writeSuccessResponse(CruiseControlParameters parameters, HttpServletResponse response) throws IOException;
}
