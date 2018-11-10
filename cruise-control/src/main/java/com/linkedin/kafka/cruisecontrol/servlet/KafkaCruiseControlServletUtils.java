/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.endPoint;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.wantJSON;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DataFrom;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;


/**
 * The util class for Kafka Cruise Control servlet.
 */
public class KafkaCruiseControlServletUtils {
  public static final int JSON_VERSION = 1;
  public static final String VERSION = "version";
  public static final String REQUEST_URI = "/KAFKACRUISECONTROL/";
  private static final String STACK_TRACE = "stackTrace";
  private static final String ERROR_MESSAGE = "errorMessage";
  private static final String PROGRESS = "progress";

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

  static String getClientIpAddress(HttpServletRequest request) {
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

  public static void setResponseCode(HttpServletResponse response, int code, boolean json) {
    response.setStatus(code);
    response.setContentType(json ? "application/json" : "text/plain");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
  }

  private static void writeResponseToOutputStream(HttpServletResponse response, int responseCode, boolean json,
                                                  String responseMsg)
      throws IOException {
    OutputStream out = response.getOutputStream();
    setResponseCode(response, responseCode, json);
    response.setContentLength(responseMsg.length());
    out.write(responseMsg.getBytes(StandardCharsets.UTF_8));
    out.flush();
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

  public static void writeErrorResponse(HttpServletResponse response,
                                        String stackTrace,
                                        String errorMessage,
                                        int responseCode,
                                        boolean json)
      throws IOException {
    String responseMsg;
    if (json) {
      Map<String, Object> exceptionMap = new HashMap<>();
      exceptionMap.put(VERSION, JSON_VERSION);
      exceptionMap.put(STACK_TRACE, stackTrace);
      exceptionMap.put(ERROR_MESSAGE, errorMessage);
      Gson gson = new Gson();
      responseMsg = gson.toJson(exceptionMap);
    } else {
      responseMsg = errorMessage == null ? "" : errorMessage;
    }
    writeResponseToOutputStream(response, responseCode, json, responseMsg);
  }

  static void returnProgress(HttpServletResponse response, OperationFuture future, boolean json) throws IOException {
    String responseMsg;
    if (json) {
      Map<String, Object> respMap = new HashMap<>();
      respMap.put(VERSION, JSON_VERSION);
      respMap.put(PROGRESS, future.getJsonArray());
      Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();
      responseMsg = gson.toJson(respMap);
    } else {
      responseMsg = future.progressString();
    }
    writeResponseToOutputStream(response, SC_OK, json, responseMsg);
  }

  static ModelCompletenessRequirements getRequirements(DataFrom dataFrom) {
    return dataFrom == DataFrom.VALID_PARTITIONS ? new ModelCompletenessRequirements(Integer.MAX_VALUE, 0.0, true)
                                                 : new ModelCompletenessRequirements(1, 1.0, true);
  }
}
