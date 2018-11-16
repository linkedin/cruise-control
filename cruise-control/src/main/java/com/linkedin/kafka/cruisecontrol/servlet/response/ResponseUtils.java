/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

import static javax.servlet.http.HttpServletResponse.SC_OK;


/**
 * The util class for Kafka Cruise Control response.
 */
public class ResponseUtils {
  public static final int JSON_VERSION = 1;
  public static final String VERSION = "version";
  public static final String MESSAGE = "message";
  private static final String PROGRESS = "progress";
  private static final String STACK_TRACE = "stackTrace";
  private static final String ERROR_MESSAGE = "errorMessage";

  private ResponseUtils() {
  }

  static void setResponseCode(HttpServletResponse response, int code, boolean json) {
    response.setStatus(code);
    response.setContentType(json ? "application/json" : "text/plain");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
  }

  static String getBaseJSONString(String message) {
    Map<String, Object> jsonResponse = new HashMap<>(2);
    jsonResponse.put(VERSION, JSON_VERSION);
    jsonResponse.put(MESSAGE, message);
    return new Gson().toJson(jsonResponse);
  }

  private static void writeResponseToOutputStream(HttpServletResponse response, int responseCode, boolean json, String responseMsg)
      throws IOException {
    OutputStream out = response.getOutputStream();
    setResponseCode(response, responseCode, json);
    response.setContentLength(responseMsg.length());
    out.write(responseMsg.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  public static void returnProgress(HttpServletResponse response, OperationFuture future, boolean json) throws IOException {
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
}
