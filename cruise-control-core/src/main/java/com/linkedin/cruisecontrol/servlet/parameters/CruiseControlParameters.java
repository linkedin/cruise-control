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
   * @return {@code true} if there has been a failure to parse parameters, {@code false} otherwise. If the object is already initialized,
   *         directly return false.
   */
  boolean parseParameters(HttpServletResponse response);

  /**
   * @return Endpoint for which the parameters are parsed.
   */
  EndPoint endPoint();

  /**
   * @return {@code true} if requested response is in JSON, {@code false} otherwise.
   */
  boolean json();

  /**
   * @return {@code true} if requested response should contain a JSON schema in the header, {@code false} otherwise.
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
