/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.IOException;
import java.io.OutputStream;
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
  public static final String MESSAGE = "message";
  private static final String STACK_TRACE = "stackTrace";
  private static final String ERROR_MESSAGE = "errorMessage";

  private ResponseUtils() {
  }

  static void setResponseCode(HttpServletResponse response, int code, boolean json, KafkaCruiseControlConfig config) {
    response.setStatus(code);
    response.setContentType(json ? "application/json" : "text/plain");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    boolean corsEnabled = config == null ? false : config.getBoolean(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
    if (corsEnabled) {
      // These headers are exposed to the browser
      response.setHeader("Access-Control-Allow-Origin",
                         config.getString(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_ORIGIN_CONFIG));
      response.setHeader("Access-Control-Expose-Headers",
                         config.getString(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG));
      response.setHeader("Access-Control-Allow-Credentials", "true");
    }
  }

  static String getBaseJSONString(String message) {
    Map<String, Object> jsonResponse = new HashMap<>(2);
    jsonResponse.put(VERSION, JSON_VERSION);
    jsonResponse.put(MESSAGE, message);
    return new Gson().toJson(jsonResponse);
  }

  private static void writeResponseToOutputStream(HttpServletResponse response,
                                                  int responseCode,
                                                  boolean json,
                                                  String responseMsg,
                                                  KafkaCruiseControlConfig config
                                                  )
      throws IOException {
    OutputStream out = response.getOutputStream();
    setResponseCode(response, responseCode, json, config);
    response.addHeader("Cruise-Control-Version", KafkaCruiseControl.cruiseControlVersion());
    response.addHeader("Cruise-Control-Commit_Id", KafkaCruiseControl.cruiseControlCommitId());
    response.setContentLength(responseMsg.length());
    out.write(responseMsg.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  public static void writeErrorResponse(HttpServletResponse response,
                                        String stackTrace,
                                        String errorMessage,
                                        int responseCode,
                                        boolean json,
                                        KafkaCruiseControlConfig config)
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
    // Send the CORS Task ID header as part of this error response if 2-step verification is enabled.
    writeResponseToOutputStream(response, responseCode, json, responseMsg, config);
  }
}
