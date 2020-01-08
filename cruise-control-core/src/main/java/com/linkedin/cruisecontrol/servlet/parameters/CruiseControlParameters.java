/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import java.util.SortedSet;
import javax.servlet.http.HttpServletResponse;


/**
 * This is the interface of the parameters used by Cruise Control. Users can implement this interface and add the
 * implementation class name to Cruise Control parameters configuration so that Cruise Control will take the parameters
 * when handling the corresponding request.
 */
public interface CruiseControlParameters extends CruiseControlConfigurable {

  /**
   * Parse the parameters set in HTTP request to initialize object.
   *
   * @param response HTTP response of Cruise Control. Populated in case of a parameter parse exception.
   * @return True if there has been a failure to parse parameters, false otherwise. If the object is already initialized,
   *         directly return false.
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

  /**
   * @return True if requested response should contain a JSON schema in the header, false otherwise.
   */
  boolean wantResponseSchema();

  /**
   * @param reviewId The review id.
   */
  void setReviewId(int reviewId);

  /**
   * @return A set of valid parameter names sorted by {@link String#CASE_INSENSITIVE_ORDER}.
   */
  SortedSet<String> caseInsensitiveParameterNames();
}
