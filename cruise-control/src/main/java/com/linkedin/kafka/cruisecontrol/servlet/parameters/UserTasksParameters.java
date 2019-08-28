/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.USER_TASK_IDS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CLIENT_IDS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ENTRIES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ENDPOINTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TYPES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.FETCH_COMPLETED_TASK_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#USER_TASKS}
 *
 * <pre>
 * Retrieve the recent user tasks.
 *    GET /kafkacruisecontrol/user_tasks?json=[true/false]&amp;user_task_ids=[Set-of-USER-TASK-IDS]&amp;client_ids=[Set-of-ClientIdentity]&amp;
 *    endpoints=[Set-of-{@link CruiseControlEndPoint}]&amp;types=[Set-of-{@link UserTaskManager.TaskState}]&amp;entries=[POSITIVE-INTEGER]
 *    &amp;fetch_completed_task=[true/false]
 * </pre>
 */
public class UserTasksParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(USER_TASK_IDS_PARAM);
    validParameterNames.add(CLIENT_IDS_PARAM);
    validParameterNames.add(ENDPOINTS_PARAM);
    validParameterNames.add(TYPES_PARAM);
    validParameterNames.add(ENTRIES_PARAM);
    validParameterNames.add(FETCH_COMPLETED_TASK_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected Set<UUID> _userTaskIds;
  protected Set<String> _clientIds;
  protected Set<CruiseControlEndPoint> _endPoints;
  protected Set<UserTaskManager.TaskState> _types;
  protected int _entries;
  protected boolean _fetchCompletedTask;

  public UserTasksParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _userTaskIds = ParameterUtils.userTaskIds(_request);
    _clientIds = ParameterUtils.clientIds(_request);
    _endPoints = ParameterUtils.endPoints(_request);
    _types = ParameterUtils.types(_request);
    _entries = ParameterUtils.entries(_request);
    _fetchCompletedTask = ParameterUtils.fetchCompletedTask(_request);
  }

  public Set<UUID> userTaskIds() {
    return _userTaskIds;
  }

  public Set<String> clientIds() {
    return _clientIds;
  }

  public Set<CruiseControlEndPoint> endPoints() {
    return _endPoints;
  }

  public Set<UserTaskManager.TaskState> types() {
    return _types;
  }

  public int entries() {
    return _entries;
  }

  public boolean fetchCompletedTask() {
    return _fetchCompletedTask;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
