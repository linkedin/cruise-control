/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


public class TrainResult extends AbstractCruiseControlResponse {
  private static final Logger LOG = LoggerFactory.getLogger(TrainResult.class);
  private static final String MESSAGE = "message";

  @Override
  protected String getJSONString(CruiseControlParameters parameters) {
    String message = String.format("Load model training started. Check status through the %s endpoint", EndPoint.STATE);
    Map<String, Object> jsonResponse = new HashMap<>();
    jsonResponse.put(VERSION, JSON_VERSION);
    jsonResponse.put(MESSAGE, message);
    return new Gson().toJson(jsonResponse);
  }

  @Override
  protected void writeOutputStream(OutputStream out, CruiseControlParameters parameters) {
    try {
      out.write(String.format("Load model training started. Check status through the %s endpoint", EndPoint.STATE)
                      .getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Failed to write output stream.", e);
    }
  }
}
