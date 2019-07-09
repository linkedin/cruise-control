/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
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
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.writeErrorResponse;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;


/**
 * The util class for Kafka Cruise Control servlet.
 */
public class KafkaCruiseControlServletUtils {
  // FIXME: Read this from a configuration
  public static final String REQUEST_URI = "/KAFKACRUISECONTROL/";
  public static final String GET_METHOD = "GET";
  public static final String POST_METHOD = "POST";

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
   * @param config The config of Cruise Control.
   * @return The endpoint if the request contains a valid one, otherwise (1) writes the error response to the given HTTP
   * response and (2) returns null.
   */
  static EndPoint getValidEndpoint(HttpServletRequest request, HttpServletResponse response, KafkaCruiseControlConfig config)
      throws IOException {
    EndPoint endPoint = endPoint(request);
    if (endPoint == null) {
      String method = request.getMethod();
      String errorMessage = String.format("Unrecognized endpoint in request '%s'%nSupported %s endpoints: %s",
                                          request.getPathInfo(), method, method.equals(GET_METHOD) ? EndPoint.getEndpoint()
                                                                                                   : EndPoint.postEndpoint());
      writeErrorResponse(response, "", errorMessage, SC_NOT_FOUND, wantJSON(request), config);
      return null;
    }
    return endPoint;
  }

  /**
   * Creates a {@link HttpServletResponse#SC_BAD_REQUEST} Http servlet response.
   */
  static String handleUserRequestException(UserRequestException ure,
                                           HttpServletRequest request,
                                           HttpServletResponse response,
                                           KafkaCruiseControlConfig config)
      throws IOException {
    String errorMessage = String.format("Bad %s request '%s' due to '%s'.", request.getMethod(), request.getPathInfo(), ure.getMessage());
    StringWriter sw = new StringWriter();
    ure.printStackTrace(new PrintWriter(sw));
    writeErrorResponse(response, sw.toString(), errorMessage, SC_BAD_REQUEST, wantJSON(request), config);
    return errorMessage;
  }

  /**
   * Creates a {@link HttpServletResponse#SC_FORBIDDEN} Http servlet response.
   */
  static String handleConfigException(ConfigException ce,
                                      HttpServletRequest request,
                                      HttpServletResponse response,
                                      KafkaCruiseControlConfig config)
      throws IOException {
    StringWriter sw = new StringWriter();
    ce.printStackTrace(new PrintWriter(sw));
    String errorMessage = String.format("Cannot process %s request '%s' due to: '%s'.",
                                        request.getMethod(), request.getPathInfo(), ce.getMessage());
    writeErrorResponse(response, sw.toString(), errorMessage, SC_FORBIDDEN, wantJSON(request), config);
    return errorMessage;
  }

  /**
   * Creates a {@link HttpServletResponse#SC_INTERNAL_SERVER_ERROR} Http servlet response.
   */
  static String handleException(Exception e,
                                HttpServletRequest request,
                                HttpServletResponse response,
                                KafkaCruiseControlConfig config)
      throws IOException {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    String errorMessage = String.format("Error processing %s request '%s' due to: '%s'.",
                                        request.getMethod(), request.getPathInfo(), e.getMessage());
    writeErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request), config);
    return errorMessage;
  }

  /**
   * Ensure that the given headerName does not exist in the given request header.
   *
   * @param request HTTP request received by Cruise Control.
   * @param headerName a <code>String</code> specifying the header name
   */
  static void ensureHeaderNotPresent(HttpServletRequest request, String headerName) {
    String value = request.getHeader(headerName);
    if (value != null) {
      throw new IllegalArgumentException(String.format("Unexpected header %s (value: %s) in the request.", value, headerName));
    }
  }

  public static String httpServletRequestToString(HttpServletRequest request) {
    return String.format("%s %s", request.getMethod(), request.getRequestURI());
  }
}
