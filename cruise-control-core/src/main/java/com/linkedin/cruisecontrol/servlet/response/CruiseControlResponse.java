/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.servlet.response;

import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;


/**
 * This is the interface of the response used by Cruise Control. Users can implement this interface and add the
 * implementation class name to Cruise Control response configuration so that Cruise Control will return the response
 * upon handling the corresponding request.
 */
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

  /**
   * Return the relevant response kept in-memory after {@link #discardIrrelevantResponse(CruiseControlParameters)} is called.
   *
   * @return The cached response, or {@code null} if the cached response is unavailable.
   */
  String cachedResponse();
}
