/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;


/**
 * The util class for Kafka Cruise Control response.
 */
public class ResponseUtils {
  public static final int JSON_VERSION = 1;
  public static final String VERSION = "version";
  @JsonResponseField
  public static final String MESSAGE = "message";

  private ResponseUtils() {
  }

  static void setResponseCode(HttpServletResponse response, int code, boolean json, KafkaCruiseControlConfig config) {
    response.setStatus(code);
    response.setContentType(json ? "application/json" : "text/plain");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    boolean corsEnabled = config == null ? false : config.getBoolean(WebServerConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
    if (corsEnabled) {
      // These headers are exposed to the browser
      response.setHeader("Access-Control-Allow-Origin",
                         config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_ORIGIN_CONFIG));
      response.setHeader("Access-Control-Expose-Headers",
                         config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG));
      response.setHeader("Access-Control-Allow-Credentials", "true");
    }
  }

  static String getBaseJSONString(String message) {
    Map<String, Object> jsonResponse = new HashMap<>(2);
    jsonResponse.put(VERSION, JSON_VERSION);
    jsonResponse.put(MESSAGE, message);
    return new Gson().toJson(jsonResponse);
  }

  static void writeResponseToOutputStream(HttpServletResponse response,
                                          int responseCode,
                                          boolean json,
                                          String responseMessage,
                                          KafkaCruiseControlConfig config)
      throws IOException {
    OutputStream out = response.getOutputStream();
    setResponseCode(response, responseCode, json, config);
    response.addHeader("Cruise-Control-Version", KafkaCruiseControl.cruiseControlVersion());
    response.addHeader("Cruise-Control-Commit_Id", KafkaCruiseControl.cruiseControlCommitId());
    response.setContentLength(responseMessage.length());
    out.write(responseMessage.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  /**
   * Retrieve stack trace (if any).
   *
   * @param e Exception from which the stack trace will be retrieved.
   * @return Stack trace if the given exception is not {@code null}, empty string otherwise.
   */
  private static String stackTrace(Exception e) {
    if (e == null) {
      return "";
    }

    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  /**
   * Write error response to the output stream.
   *
   * @param response HTTP response to return to user.
   * @param e Exception (if any) corresponding to the error, {@code null} otherwise.
   * @param errorMessage Error message to return in the response message.
   * @param responseCode HTTP Status code to indicate the error.
   * @param json True if json, false otherwise.
   * @param config The configurations for Cruise Control.
   */
  public static void writeErrorResponse(HttpServletResponse response,
                                        Exception e,
                                        String errorMessage,
                                        int responseCode,
                                        boolean json,
                                        KafkaCruiseControlConfig config)
      throws IOException {
    String responseMessage;
    ErrorResponse errorResponse = new ErrorResponse(e, errorMessage);
    if (json) {
      Gson gson = new Gson();
      responseMessage = gson.toJson(errorResponse.getJsonStructure());
    } else {
      responseMessage = errorResponse.toString();
    }
    // Send the CORS Task ID header as part of this error response if 2-step verification is enabled.
    writeResponseToOutputStream(response, responseCode, json, responseMessage, config);
  }

  @JsonResponseClass
  public static class ErrorResponse {
    @JsonResponseField
    private static final String STACK_TRACE = "stackTrace";
    @JsonResponseField
    private static final String ERROR_MESSAGE = "errorMessage";
    private Exception _exception;
    private String _errorMessage;

    ErrorResponse(Exception exception, String errorMessage) {
      _errorMessage = errorMessage;
      _exception = exception;
    }

    @Override
    public String toString() {
      return _errorMessage == null ? "" : _errorMessage;
    }

    /**
     * @return The map describing the error.
     */
    public Map<String, Object> getJsonStructure() {
      Map<String, Object> exceptionMap = new HashMap<>();
      exceptionMap.put(VERSION, JSON_VERSION);
      exceptionMap.put(STACK_TRACE, stackTrace(_exception));
      exceptionMap.put(ERROR_MESSAGE, _errorMessage);
      return exceptionMap;
    }
  }
}
