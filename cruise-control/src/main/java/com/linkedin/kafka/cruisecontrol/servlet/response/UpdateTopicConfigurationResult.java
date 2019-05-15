/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.*;


public class UpdateTopicConfigurationResult extends AbstractCruiseControlResponse {
  private final Set<String> _topics;
  private final int _newReplicationFactor;
  private static final String TOPICS = "topics";
  private static final String NEW_REPLICATION_FACTOR = "newReplicationFactor";
  public UpdateTopicConfigurationResult(Set<String> topics,
                                        int newReplicationFactor,
                                        KafkaCruiseControlConfig config) {
    super(config);
    _topics = topics;
    _newReplicationFactor = newReplicationFactor;
  }

  private String getJSONstring() {
    Map<String, Object> result = new HashMap<>();
    result.put(TOPICS, _topics);
    result.put(NEW_REPLICATION_FACTOR, _newReplicationFactor);
    result.put(VERSION, JSON_VERSION);
    Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();
    return gson.toJson(result);
  }

  private String getPlaintext() {
    return String.format("The replication factor for topics %s is increased to %d.", _topics, _newReplicationFactor);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONstring() : getPlaintext();
    _topics.clear();
  }
}
