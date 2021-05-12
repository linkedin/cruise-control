/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Request;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * A custom {@link ConstraintSecurityHandler} that converts the request to lowercase to ensure case insensitivity.
 */
public class CruiseControlSecurityHandler extends ConstraintSecurityHandler {

  @Override
  public void handle(String pathInContext, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    super.handle(pathInContext.toLowerCase(), baseRequest, request, response);
  }
}
