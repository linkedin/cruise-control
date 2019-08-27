/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.servlet.handler;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * This is the interface of the request handled by Cruise Control. Users can implement this interface and add the
 * implementation class name to Cruise Control request configuration so that Cruise Control will handle the corresponding
 * request as specified in the custom logic.
 */
public interface Request extends CruiseControlConfigurable {

  /**
   * Handle the request and populate the response.
   *
   * @param request Http servlet request.
   * @param response Http servlet response.
   */
  void handle(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException;
}
