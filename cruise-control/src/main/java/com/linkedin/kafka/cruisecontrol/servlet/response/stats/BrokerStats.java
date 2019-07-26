/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.AbstractCruiseControlResponse;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


/**
 * Get broker level stats in human readable format.
 */
public abstract class BrokerStats extends AbstractCruiseControlResponse {
  protected static final String BROKERS = "brokers";
  protected int _hostFieldLength;
  protected String _cachedPlainTextResponse;
  protected String _cachedJSONResponse;
  protected boolean _isBrokerStatsEstimated;

  public BrokerStats(KafkaCruiseControlConfig config) {
    super(config);
    _hostFieldLength = 0;
    _cachedPlainTextResponse = null;
    _cachedJSONResponse = null;
    _isBrokerStatsEstimated = false;
  }

  public boolean isBrokerStatsEstimated() {
    return _isBrokerStatsEstimated;
  }

  private String getJSONString() {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure();
    jsonStructure.put(VERSION, JSON_VERSION);
    return gson.toJson(jsonStructure);
  }

  /**
   * Return an object that can be further be used to encode into JSON
   */
  public abstract Map<String, Object> getJsonStructure();
  protected abstract void discardIrrelevantResponse();

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedJSONResponse = getJSONString();
    _cachedPlainTextResponse = toString();
    discardIrrelevantResponse();
  }

  @Override
  public void discardIrrelevantResponse(CruiseControlParameters parameters) {
    if (_cachedJSONResponse == null || _cachedPlainTextResponse == null) {
      discardIrrelevantAndCacheRelevant(parameters);
      if (_cachedJSONResponse == null || _cachedPlainTextResponse == null) {
        throw new IllegalStateException("Failed to cache the relevant response.");
      }
    }
    _cachedResponse = parameters.json() ? _cachedJSONResponse : _cachedPlainTextResponse;
  }
}
