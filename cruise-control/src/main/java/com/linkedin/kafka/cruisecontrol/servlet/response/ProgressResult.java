/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

public class ProgressResult extends AbstractCruiseControlResponse {
  private static final String PROGRESS = "progress";
  private static final String OPERATION = "operation";
  private static final String OPERATION_PROGRESS = "operationProgress";
  private final List<OperationFuture> _futures;

  public ProgressResult(List<OperationFuture> futures, KafkaCruiseControlConfig config) {
    super(config);
    _futures = futures;
  }

  private String getJSONString() {
    Map<String, Object> jsonResponse = new HashMap<>(2);
    jsonResponse.put(VERSION, JSON_VERSION);
    List<Object> progress = new ArrayList<>(_futures.size());
    for (OperationFuture future: _futures) {
      Map<String, Object> operationProgress = new HashMap<>(2);
      operationProgress.put(OPERATION, future.operation());
      operationProgress.put(OPERATION_PROGRESS, future.getJsonArray());
      progress.add(operationProgress);
    }
    jsonResponse.put(PROGRESS, progress);
    Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();

    return gson.toJson(jsonResponse);
  }

  private String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    for (OperationFuture operationFuture: _futures) {
      sb.append(String.format("%s:%n%s", operationFuture.operation(), operationFuture.progressString()));
    }
    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString() : getPlaintext();
    // Discard irrelevant response.
    _futures.clear();
  }
}
