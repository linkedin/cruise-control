/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.ProvisionerState;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.*;


@JsonResponseClass
public class RightsizeResult extends AbstractCruiseControlResponse {
  @JsonResponseField
  protected static final String NUM_BROKERS_TO_ADD = "numBrokersToAdd";
  @JsonResponseField
  protected static final String PARTITION_COUNT = "partitionCount";
  @JsonResponseField
  protected static final String TOPIC = "topic";
  @JsonResponseField
  protected static final String PROVISIONER_STATE = "provisionerState";
  @JsonResponseField
  protected static final String PROVISIONER_SUMMARY = "provisionerSummary";

  protected final int _numBrokersToAdd;
  protected final int _partitionCount;
  protected String _topic;
  protected String _provisionState;
  protected String _provisionSummary;

  public RightsizeResult(int numBrokersToAdd,
                         int partitionCount,
                         String topic,
                         ProvisionerState provisionerState,
                         KafkaCruiseControlConfig config) {
    super(config);

    _numBrokersToAdd = numBrokersToAdd;
    _partitionCount = partitionCount;
    _topic = topic;
    _provisionState = provisionerState.state().toString();
    _provisionSummary = provisionerState.summary();
  }

  protected String getJSONString() {
    Map<String, Object> jsonStructure = new HashMap<>(5);
    jsonStructure.put(NUM_BROKERS_TO_ADD, _numBrokersToAdd);
    jsonStructure.put(PARTITION_COUNT, _partitionCount);
    if (_topic != null && !_topic.isEmpty()) {
      jsonStructure.put(TOPIC, _topic);
    }
    if (_provisionState != null && !_provisionState.isEmpty()) {
      jsonStructure.put(PROVISIONER_STATE, _provisionState);
    }
    if (_provisionSummary != null && !_provisionSummary.isEmpty()) {
      jsonStructure.put(PROVISIONER_SUMMARY, _provisionSummary);
    }
    jsonStructure.put(VERSION, JSON_VERSION);
    Gson gson = new Gson();
    return gson.toJson(jsonStructure);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    _cachedResponse = getJSONString();
    // Discard irrelevant response.
    _topic = null;
    _provisionState = null;
    _provisionSummary = null;
  }
}
