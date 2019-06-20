/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public interface Request {

  /**
   * Handle the request and populate the response.
   *
   * @param request Http servlet request.
   * @param response Http servlet response.
   */
  void handle(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ExecutionException, InterruptedException;
}
