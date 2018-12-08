/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.setResponseCode;
import static javax.servlet.http.HttpServletResponse.SC_OK;


public abstract class AbstractCruiseControlResponse implements CruiseControlResponse {
  protected String _cachedResponse;

  public AbstractCruiseControlResponse() {
    _cachedResponse = null;
  }

  protected abstract void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters);

  @Override
  public void writeSuccessResponse(CruiseControlParameters parameters, HttpServletResponse response) throws IOException {
    OutputStream out = response.getOutputStream();
    boolean json = parameters.json();
    setResponseCode(response, SC_OK, json, null);
    response.addHeader("Cruise-Control-Version", KafkaCruiseControl.cruiseControlVersion());
    response.addHeader("Cruise-Control-Commit_Id", KafkaCruiseControl.cruiseControlCommitId());
    discardIrrelevantResponse(parameters);
    response.setContentLength(_cachedResponse.length());
    out.write(_cachedResponse.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  @Override
  public void discardIrrelevantResponse(CruiseControlParameters parameters) {
    if (_cachedResponse == null) {
      discardIrrelevantAndCacheRelevant(parameters);
      if (_cachedResponse == null) {
        throw new IllegalStateException("Failed to cache the relevant response.");
      }
    }
  }
}
