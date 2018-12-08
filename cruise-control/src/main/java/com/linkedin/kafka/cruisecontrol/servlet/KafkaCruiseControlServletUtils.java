/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.endPoint;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.wantJSON;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DataFrom;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.writeErrorResponse;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;


/**
 * The util class for Kafka Cruise Control servlet.
 */
public class KafkaCruiseControlServletUtils {
  // FIXME: Read this from a configuration
  public static final String REQUEST_URI = "/KAFKACRUISECONTROL/";

  private KafkaCruiseControlServletUtils() {

  }

  static final String[] HEADERS_TO_TRY = {
      "X-Forwarded-For",
      "Proxy-Client-IP",
      "WL-Proxy-Client-IP",
      "HTTP_X_FORWARDED_FOR",
      "HTTP_X_FORWARDED",
      "HTTP_X_CLUSTER_CLIENT_IP",
      "HTTP_CLIENT_IP",
      "HTTP_FORWARDED_FOR",
      "HTTP_FORWARDED",
      "HTTP_VIA",
      "REMOTE_ADDR"
  };

  public static String getClientIpAddress(HttpServletRequest request) {
    for (String header : HEADERS_TO_TRY) {
      String ip = request.getHeader(header);
      if (ip != null && ip.length() != 0 && !"unknown".equalsIgnoreCase(ip)) {
        return ip;
      }
    }
    return request.getRemoteAddr();
  }

  static String urlEncode(String s) throws UnsupportedEncodingException {
    return s == null ? null : URLEncoder.encode(s, StandardCharsets.UTF_8.name());
  }

  /**
   * Returns the endpoint if the request contains a valid one, otherwise (1) writes the error response to the given HTTP
   * response and (2) returns null.
   *
   * @param request HTTP request received by Cruise Control.
   * @param response HTTP response of Cruise Control.
   * @return The endpoint if the request contains a valid one, otherwise (1) writes the error response to the given HTTP
   * response and (2) returns null.
   */
  static EndPoint getValidEndpoint(HttpServletRequest request, HttpServletResponse response) throws IOException {
    EndPoint endPoint = endPoint(request);
    if (endPoint == null) {
      String method = request.getMethod();
      String errorMessage = String.format("Unrecognized endpoint in request '%s'%nSupported %s endpoints: %s",
                                          request.getPathInfo(), method, method.equals("GET") ? EndPoint.getEndpoint()
                                                                                              : EndPoint.postEndpoint());
      writeErrorResponse(response, "", errorMessage, SC_NOT_FOUND, wantJSON(request));
      return null;
    }
    return endPoint;
  }

  static void handleUserRequestException(UserRequestException ure, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String errorMessage = String.format("Bad %s request '%s'", request.getMethod(), request.getPathInfo());
    StringWriter sw = new StringWriter();
    ure.printStackTrace(new PrintWriter(sw));
    writeErrorResponse(response, sw.toString(), errorMessage, SC_BAD_REQUEST, wantJSON(request));
  }

  static ModelCompletenessRequirements getRequirements(DataFrom dataFrom) {
    return dataFrom == DataFrom.VALID_PARTITIONS ? new ModelCompletenessRequirements(Integer.MAX_VALUE, 0.0, true)
                                                 : new ModelCompletenessRequirements(1, 1.0, true);
  }
}
