/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;


/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.EndPoint#USER_TASKS}
 *
 * <pre>
 * Retrieve the recent user tasks.
 *    GET /kafkacruisecontrol/user_tasks?json=[true/false]&amp;user_task_ids=[USER-TASK-IDS]
 * </pre>
 */
public class UserTasksParameters extends AbstractParameters {
  private Set<UUID> _userTaskIds;
  private Set<String> _clientIds;
  private Set<EndPoint> _endPoints;
  private Set<UserTaskManager.TaskState> _taskStates;
  private int _entries;

  public UserTasksParameters(HttpServletRequest request) {
    super(request);
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _userTaskIds = ParameterUtils.userTaskIds(_request);
    _clientIds = ParameterUtils.userTaskClientIds(_request);
    _endPoints = ParameterUtils.userTaskEndPoints(_request);
    _taskStates = ParameterUtils.userTaskState(_request);
    _entries = ParameterUtils.entries(_request);
  }

  public Set<UUID> userTaskIds() {
    return _userTaskIds;
  }

  public Set<String> clientIds() {
    return _clientIds;
  }

  public Set<EndPoint> endPoints() {
    return _endPoints;
  }

  public Set<UserTaskManager.TaskState> taskStates() {
    return _taskStates;
  }

  public int listLength() {
    return _entries;
  }
}
