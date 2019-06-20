/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.UserTaskState;
import java.util.List;


public class UserTasksRequest extends AbstractSyncRequest {
  private final List<UserTaskManager.UserTaskInfo> _userTasks;
  private final KafkaCruiseControlConfig _config;
  private final UserTasksParameters _parameters;

  public UserTasksRequest(KafkaCruiseControlServlet servlet, UserTasksParameters parameters) {
    super(servlet);
    _userTasks = servlet.getAllUserTasks();
    _config = servlet.asyncKafkaCruiseControl().config();
    _parameters = parameters;
  }

  @Override
  protected UserTaskState handle() {
    return new UserTaskState(_userTasks, _config);
  }

  @Override
  public UserTasksParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return UserTasksRequest.class.getSimpleName();
  }
}
