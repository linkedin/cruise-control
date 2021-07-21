/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */
package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.Map;
import java.util.HashMap;

@JsonResponseClass
public class GoalReadinessRecord {
  @JsonResponseField
  protected static final String NAME = "name";
  @JsonResponseField
  protected static final String MODEL_COMPLETE_REQUIREMENT = "modelCompleteRequirement";
  @JsonResponseField
  protected static final String STATUS = "status";

  protected Goal _goal;
  protected String _status;

  GoalReadinessRecord(Goal goal, String goalReadyStatus) {
    _goal = goal;
    _status = goalReadyStatus;
  }

  protected Map<String, Object> getJsonStructure() {
    Map<String, Object> goalReadinessRecord = new HashMap<>(3);
    goalReadinessRecord.put(NAME, _goal.getClass().getSimpleName());
    goalReadinessRecord.put(MODEL_COMPLETE_REQUIREMENT,
        _goal.clusterModelCompletenessRequirements().getJsonStructure());
    goalReadinessRecord.put(STATUS, _status);
    return goalReadinessRecord;
  }
}
