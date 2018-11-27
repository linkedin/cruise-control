/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


public class UserTaskState extends AbstractCruiseControlResponse {
  private static final String DATA_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  private static final String TIME_ZONE = "UTC";
  private static final String ACTIVE_TASK_LABEL_VALUE = "Active";
  private static final String COMPLETED_TASK_LABEL_VALUE = "Completed";
  private static final String USER_TASK_ID = "UserTaskId";
  private static final String REQUEST_URL = "RequestURL";
  private static final String CLIENT_ID = "ClientIdentity";
  private static final String START_MS = "StartMs";
  private static final String STATUS = "Status";
  private static final String USER_TASKS = "userTasks";
  private final List<UserTaskManager.UserTaskInfo> _activeUserTasks;
  private final List<UserTaskManager.UserTaskInfo> _completedUserTasks;

  public UserTaskState(List<UserTaskManager.UserTaskInfo> activeUserTasks,
                       List<UserTaskManager.UserTaskInfo> completedUserTasks) {
    _activeUserTasks = activeUserTasks;
    _completedUserTasks = completedUserTasks;
  }

  private String getJSONString(CruiseControlParameters parameters) {
    List<Map<String, Object>> jsonUserTaskList = new ArrayList<>();

    Set<UUID> requestedUserTaskIds = ((UserTasksParameters) parameters).userTaskIds();
    addFilteredJSONTasks(jsonUserTaskList, _activeUserTasks, ACTIVE_TASK_LABEL_VALUE, requestedUserTaskIds);
    addFilteredJSONTasks(jsonUserTaskList, _completedUserTasks, COMPLETED_TASK_LABEL_VALUE, requestedUserTaskIds);

    Map<String, Object> jsonResponse = new HashMap<>();
    jsonResponse.put(USER_TASKS, jsonUserTaskList);
    jsonResponse.put(VERSION, JSON_VERSION);
    return new Gson().toJson(jsonResponse);
  }

  private void addJSONTask(List<Map<String, Object>> jsonUserTaskList,
                           UserTaskManager.UserTaskInfo userTaskInfo,
                           String status) {
    Map<String, Object> jsonObjectMap = new HashMap<>();
    jsonObjectMap.put(USER_TASK_ID, userTaskInfo.userTaskId().toString());
    jsonObjectMap.put(REQUEST_URL, userTaskInfo.requestWithParams());
    jsonObjectMap.put(CLIENT_ID, userTaskInfo.clientIdentity());
    jsonObjectMap.put(START_MS, Long.toString(userTaskInfo.startMs()));
    jsonObjectMap.put(STATUS, status);
    jsonUserTaskList.add(jsonObjectMap);
  }

  private void addFilteredJSONTasks(List<Map<String, Object>> jsonUserTaskList,
                                    List<UserTaskManager.UserTaskInfo> userTasks,
                                    String status,
                                    Set<UUID> requestedUserTaskIds) {
    for (UserTaskManager.UserTaskInfo userTaskInfo : userTasks) {
      if (requestedUserTaskIds == null || requestedUserTaskIds.isEmpty() || requestedUserTaskIds.contains(userTaskInfo.userTaskId())) {
        addJSONTask(jsonUserTaskList, userTaskInfo, status);
      }
    }
  }

  private String getPlaintext(CruiseControlParameters parameters) {
    Set<UUID> requestedUserTaskIds = ((UserTasksParameters) parameters).userTaskIds();
    StringBuilder sb = new StringBuilder();
    int padding = 2;
    int userTaskIdLabelSize = 20;
    int clientAddressLabelSize = 20;
    int startMsLabelSize = 20;
    int statusLabelSize = 10;
    int requestURLLabelSize = 20;

    Map<String, List<UserTaskManager.UserTaskInfo>> taskTypeMap = new TreeMap<>();
    taskTypeMap.put(ACTIVE_TASK_LABEL_VALUE, _activeUserTasks);
    taskTypeMap.put(COMPLETED_TASK_LABEL_VALUE, _completedUserTasks);

    for (List<UserTaskManager.UserTaskInfo> taskList : taskTypeMap.values()) {
      for (UserTaskManager.UserTaskInfo userTaskInfo : taskList) {
        userTaskIdLabelSize =
            userTaskIdLabelSize < userTaskInfo.userTaskId().toString().length() ? userTaskInfo.userTaskId()
                                                                                              .toString()
                                                                                              .length()
                                                                                : userTaskIdLabelSize;
        clientAddressLabelSize =
            clientAddressLabelSize < userTaskInfo.clientIdentity().length() ? userTaskInfo.clientIdentity().length()
                                                                            : clientAddressLabelSize;
        Date date = new Date(userTaskInfo.startMs());
        DateFormat formatter = new SimpleDateFormat(DATA_FORMAT);
        formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
        String dateFormatted = formatter.format(date);
        startMsLabelSize = startMsLabelSize < dateFormatted.length() ? dateFormatted.length() : startMsLabelSize;
        requestURLLabelSize =
            requestURLLabelSize < userTaskInfo.requestWithParams().length() ? userTaskInfo.requestWithParams()
                                                                                          .length()
                                                                            : requestURLLabelSize;
      }
    }

    StringBuilder formattingStringBuilder = new StringBuilder("%n%-");
    formattingStringBuilder.append(userTaskIdLabelSize + padding)
                           .append("s%-")
                           .append(clientAddressLabelSize + padding)
                           .append("s%-")
                           .append(startMsLabelSize + padding)
                           .append("s%-")
                           .append(statusLabelSize + padding)
                           .append("s%-")
                           .append(requestURLLabelSize + padding)
                           .append("s");

    sb.append(String.format(formattingStringBuilder.toString(), "USER TASK ID", "CLIENT ADDRESS", "START TIME", "STATUS",
                            "REQUEST URL")); // header
    for (Map.Entry<String, List<UserTaskManager.UserTaskInfo>> entry : taskTypeMap.entrySet()) {
      for (UserTaskManager.UserTaskInfo userTaskInfo : entry.getValue()) {
        if (requestedUserTaskIds == null || requestedUserTaskIds.isEmpty()
            || requestedUserTaskIds.contains(userTaskInfo.userTaskId())) {
          Date date = new Date(userTaskInfo.startMs());
          DateFormat formatter = new SimpleDateFormat(DATA_FORMAT);
          formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
          String dateFormatted = formatter.format(date);
          sb.append(String.format(formattingStringBuilder.toString(), userTaskInfo.userTaskId().toString(), userTaskInfo.clientIdentity(),
                                  dateFormatted, entry.getKey(), userTaskInfo.requestWithParams())); // values
        }
      }
    }

    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString(parameters) : getPlaintext(parameters);
    // Discard irrelevant response.
    _activeUserTasks.clear();
    _completedUserTasks.clear();
  }
}
