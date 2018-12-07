/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
  private static final String OPERATION = "operation";
  private static final String OPERATION_PROGRESS = "operationProgress";
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
      response.setHeader("Access-Control-Expose-Headers",
            config.getString(KafkaCruiseControlConfig.WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG));

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

  public static void returnProgress(HttpServletResponse response,
                                    List<OperationFuture> futures,
                                    boolean json,
                                    KafkaCruiseControlConfig config)
      throws IOException {
    String responseMsg;
    if (json) {
      Map<String, Object> respMap = new HashMap<>(2);
      respMap.put(VERSION, JSON_VERSION);
      List<Object> progress = new ArrayList<>(futures.size());
      for (OperationFuture future: futures) {
        Map<String, Object> operationProgress = new HashMap<>(2);
        operationProgress.put(OPERATION, future.operation());
        operationProgress.put(OPERATION_PROGRESS, future.getJsonArray());
        progress.add(operationProgress);
      }
      respMap.put(PROGRESS, progress);
      Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();
      responseMsg = gson.toJson(respMap);
    } else {
      StringBuilder sb = new StringBuilder();
      for (OperationFuture operationFuture: futures) {
        sb.append(String.format("%s:%n%s", operationFuture.operation(), operationFuture.progressString()));
      }
      responseMsg = sb.toString();
    }
    // We need the Task ID CORS header as part of the response
    // in case the server supports CORS so that web-ui can read this
    // task-id and query for future progress.
    writeResponseToOutputStream(response, SC_OK, json, responseMsg, config);
  }

  public static void writeErrorResponse(HttpServletResponse response,
                                        String stackTrace,
                                        String errorMessage,
                                        int responseCode,
                                        boolean json
                                        )
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
    // We don't need to send the CORS Task ID header as part of this
    // error response
    writeResponseToOutputStream(response, responseCode, json, responseMsg, null);
  }
}
