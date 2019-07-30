/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


public class AdminResult extends AbstractCruiseControlResponse {
  protected static final String SELF_HEALING_ENABLED_BEFORE = "selfHealingEnabledBefore";
  protected static final String SELF_HEALING_ENABLED_AFTER = "selfHealingEnabledAfter";
  protected static final String ONGOING_CONCURRENCY_CHANGE_REQUEST = "ongoingConcurrencyChangeRequest";
  protected static final String DROP_RECENT_BROKERS_REQUEST = "dropRecentBrokersRequest";
  protected final Map<AnomalyType, Boolean> _selfHealingEnabledBefore;
  protected final Map<AnomalyType, Boolean> _selfHealingEnabledAfter;
  protected String _ongoingConcurrencyChangeRequest;
  protected String _dropRecentBrokersRequest;

  public AdminResult(Map<AnomalyType, Boolean> selfHealingEnabledBefore,
                     Map<AnomalyType, Boolean> selfHealingEnabledAfter,
                     String ongoingConcurrencyChangeRequest,
                     String dropRecentBrokersRequest,
                     KafkaCruiseControlConfig config) {
    super(config);
    _selfHealingEnabledBefore = selfHealingEnabledBefore;
    _selfHealingEnabledAfter = selfHealingEnabledAfter;
    _ongoingConcurrencyChangeRequest = ongoingConcurrencyChangeRequest;
    _dropRecentBrokersRequest = dropRecentBrokersRequest;
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString() : getPlaintext();
    // Discard irrelevant response.
    _selfHealingEnabledBefore.clear();
    _selfHealingEnabledAfter.clear();
    _ongoingConcurrencyChangeRequest = null;
    _dropRecentBrokersRequest = null;
  }

  protected String getJSONString() {
    // Set initial capacity to max possible capacity to avoid rehashing.
    Map<String, Object> jsonStructure = new HashMap<>(4);
    if (!_selfHealingEnabledBefore.isEmpty()) {
      jsonStructure.put(SELF_HEALING_ENABLED_BEFORE, _selfHealingEnabledBefore);
      jsonStructure.put(SELF_HEALING_ENABLED_AFTER, _selfHealingEnabledAfter);
    }
    if (_ongoingConcurrencyChangeRequest != null) {
      jsonStructure.put(ONGOING_CONCURRENCY_CHANGE_REQUEST, _ongoingConcurrencyChangeRequest);
    }
    if (_dropRecentBrokersRequest != null) {
      jsonStructure.put(DROP_RECENT_BROKERS_REQUEST, _dropRecentBrokersRequest);
    }
    jsonStructure.put(VERSION, JSON_VERSION);
    return new Gson().toJson(jsonStructure);
  }

  protected String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("{%n"));
    if (!_selfHealingEnabledBefore.isEmpty()) {
      sb.append(String.format("%s: %s, %s: %s%n", SELF_HEALING_ENABLED_BEFORE, _selfHealingEnabledBefore,
                              SELF_HEALING_ENABLED_AFTER, _selfHealingEnabledAfter));
    }
    if (_ongoingConcurrencyChangeRequest != null) {
      sb.append(String.format("%s: %s%n", ONGOING_CONCURRENCY_CHANGE_REQUEST, _ongoingConcurrencyChangeRequest));
    }
    if (_dropRecentBrokersRequest != null) {
      sb.append(String.format("%s: %s%n", DROP_RECENT_BROKERS_REQUEST, _dropRecentBrokersRequest));
    }
    sb.append("}");

    return sb.toString();
  }
}
