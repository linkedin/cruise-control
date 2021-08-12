/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.ProvisionerState;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;


@JsonResponseClass
public class RightsizeResult extends AbstractCruiseControlResponse {
  @JsonResponseField(required = false)
  protected static final String NUM_BROKERS_TO_ADD = "numBrokersToAdd";
  @JsonResponseField(required = false)
  protected static final String PARTITION_COUNT = "partitionCount";
  @JsonResponseField(required = false)
  protected static final String TOPIC = "topic";
  @JsonResponseField
  protected static final String PROVISIONER_STATE = "provisionerState";

  protected final int _numBrokersToAdd;
  protected final int _partitionCount;
  protected String _topic;
  protected String _provisionerState;

  public RightsizeResult(int numBrokersToAdd,
                         int partitionCount,
                         String topic,
                         ProvisionerState provisionerState,
                         KafkaCruiseControlConfig config) {
    super(config);

    _numBrokersToAdd = numBrokersToAdd;
    _partitionCount = partitionCount;
    _topic = topic;
    _provisionerState = provisionerState.toString();
  }

  protected String getJSONString() {
    Map<String, Object> jsonStructure = new HashMap<>(5);
    if (_numBrokersToAdd != ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
      jsonStructure.put(NUM_BROKERS_TO_ADD, _numBrokersToAdd);
    }
    if (_partitionCount != ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
      jsonStructure.put(PARTITION_COUNT, _partitionCount);
    }
    if (_topic != null && !_topic.isEmpty()) {
      jsonStructure.put(TOPIC, _topic);
    }
    jsonStructure.put(PROVISIONER_STATE, _provisionerState);
    jsonStructure.put(VERSION, JSON_VERSION);
    Gson gson = new Gson();
    return gson.toJson(jsonStructure);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    _cachedResponse = getJSONString();
    // Discard irrelevant response.
    _topic = null;
    _provisionerState = null;
  }
}
