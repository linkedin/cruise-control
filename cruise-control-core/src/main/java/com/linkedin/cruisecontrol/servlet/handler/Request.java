/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.servlet.handler;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;


/**
 * This is the interface of the request handled by Cruise Control. Users can implement this interface and add the
 * implementation class name to Cruise Control request configuration so that Cruise Control will handle the corresponding
 * request as specified in the custom logic.
 */
public interface Request extends CruiseControlConfigurable {

  /**
   * @param requestContext The request context
   * Handle the request and populate the response.
   */
  void handle(CruiseControlRequestContext requestContext)
       throws Exception;
}
