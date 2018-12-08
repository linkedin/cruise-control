/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


public class AdminResult extends AbstractCruiseControlResponse {
  private static final String SELF_HEALING_ENABLED_BEFORE = "selfHealingEnabledBefore";
  private static final String SELF_HEALING_ENABLED_AFTER = "selfHealingEnabledAfter";
  private static final String ONGOING_CONCURRENCY_CHANGE_REQUEST = "ongoingConcurrencyChangeRequest";
  private final Map<AnomalyType, Boolean> _selfHealingEnabledBefore;
  private final Map<AnomalyType, Boolean> _selfHealingEnabledAfter;
  private String _ongoingConcurrencyChangeRequest;

  public AdminResult(Map<AnomalyType, Boolean> selfHealingEnabledBefore,
                     Map<AnomalyType, Boolean> selfHealingEnabledAfter,
                     String ongoingConcurrencyChangeRequest) {
    _selfHealingEnabledBefore = selfHealingEnabledBefore;
    _selfHealingEnabledAfter = selfHealingEnabledAfter;
    _ongoingConcurrencyChangeRequest = ongoingConcurrencyChangeRequest;
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString() : getPlaintext();
    // Discard irrelevant response.
    _selfHealingEnabledBefore.clear();
    _selfHealingEnabledAfter.clear();
    _ongoingConcurrencyChangeRequest = null;
  }

  private String getJSONString() {
    // Set initial capacity to max possible capacity to avoid rehashing.
    Map<String, Object> jsonStructure = new HashMap<>(4);
    if (!_selfHealingEnabledBefore.isEmpty()) {
      jsonStructure.put(SELF_HEALING_ENABLED_BEFORE, _selfHealingEnabledBefore);
      jsonStructure.put(SELF_HEALING_ENABLED_AFTER, _selfHealingEnabledAfter);
    }
    if (_ongoingConcurrencyChangeRequest != null) {
      jsonStructure.put(ONGOING_CONCURRENCY_CHANGE_REQUEST, _ongoingConcurrencyChangeRequest);
    }
    jsonStructure.put(VERSION, JSON_VERSION);
    return new Gson().toJson(jsonStructure);
  }

  private String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    sb.append("{%n");
    if (!_selfHealingEnabledBefore.isEmpty()) {
      sb.append(String.format("%s: %s, %s: %s%n", SELF_HEALING_ENABLED_BEFORE, _selfHealingEnabledBefore,
                              SELF_HEALING_ENABLED_AFTER, _selfHealingEnabledAfter));
    }
    if (_ongoingConcurrencyChangeRequest != null) {
      sb.append(String.format("%s: %s%n", ONGOING_CONCURRENCY_CHANGE_REQUEST, _ongoingConcurrencyChangeRequest));
    }
    sb.append("}");

    return sb.toString();
  }
}
