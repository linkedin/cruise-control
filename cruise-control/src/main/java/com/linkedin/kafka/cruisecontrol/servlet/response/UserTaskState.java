/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


public class UserTaskState extends AbstractCruiseControlResponse {
  private static final String DATE_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  private static final String TIME_ZONE = "UTC";
  private static final String ACTIVE_TASK_LABEL_VALUE = UserTaskManager.TaskState.ACTIVE.type();
  private static final String COMPLETED_TASK_LABEL_VALUE = UserTaskManager.TaskState.COMPLETED.type();
  private static final String USER_TASK_ID = "UserTaskId";
  private static final String REQUEST_URL = "RequestURL";
  private static final String CLIENT_ID = "ClientIdentity";
  private static final String START_MS = "StartMs";
  private static final String STATUS = "Status";
  private static final String USER_TASKS = "userTasks";
  private final Map<UserTaskManager.TaskState, List<UserTaskManager.UserTaskInfo>> _userTasksByTaskState;

  public UserTaskState(List<UserTaskManager.UserTaskInfo> activeUserTasks,
                       List<UserTaskManager.UserTaskInfo> completedUserTasks) {
    _userTasksByTaskState = new HashMap<>(2);
    _userTasksByTaskState.put(UserTaskManager.TaskState.ACTIVE, activeUserTasks);
    _userTasksByTaskState.put(UserTaskManager.TaskState.COMPLETED, completedUserTasks);
  }

  private String getJSONString(CruiseControlParameters parameters) {
    List<Map<String, Object>> jsonUserTaskList = new ArrayList<>();
    for (UserTaskManager.UserTaskInfo taskInfo : prepareResultList(parameters)) {
      addJSONTask(jsonUserTaskList, taskInfo);
    }
    Map<String, Object> jsonResponse = new HashMap<>();
    jsonResponse.put(USER_TASKS, jsonUserTaskList);
    jsonResponse.put(VERSION, JSON_VERSION);
    return new Gson().toJson(jsonResponse);
  }

  // Also used for testing
  public List<UserTaskManager.UserTaskInfo> prepareResultList(CruiseControlParameters parameters) {
    int entries = ((UserTasksParameters) parameters).entries();
    // If entries argument isn't given in request, we give MAX_VALUE to entries. Thus need to avoid instantiating
    // to MAX_VALUE
    List<UserTaskManager.UserTaskInfo> resultList = (entries == Integer.MAX_VALUE ? new ArrayList<>() : new ArrayList<>(entries));

    // We fill result with ACTIVE tasks first and then COMPLETED tasks
    populateFilteredTasks(resultList, _userTasksByTaskState.get(UserTaskManager.TaskState.ACTIVE), parameters, entries);
    populateFilteredTasks(resultList, _userTasksByTaskState.get(UserTaskManager.TaskState.COMPLETED), parameters, entries);
    return resultList.subList(0, Math.min(entries, resultList.size()));
  }

  private static String getStatus(UserTaskManager.UserTaskInfo userTaskInfo) {
    switch (userTaskInfo.state()) {
      case ACTIVE:
        return ACTIVE_TASK_LABEL_VALUE;
      case COMPLETED:
        return COMPLETED_TASK_LABEL_VALUE;
      default:
        throw new IllegalStateException("Unrecognized state " + userTaskInfo.state());
    }
  }

  private void addJSONTask(List<Map<String, Object>> jsonUserTaskList,
                           UserTaskManager.UserTaskInfo userTaskInfo) {
    Map<String, Object> jsonObjectMap = new HashMap<>();
    String status = getStatus(userTaskInfo);
    jsonObjectMap.put(USER_TASK_ID, userTaskInfo.userTaskId().toString());
    jsonObjectMap.put(REQUEST_URL, userTaskInfo.requestWithParams());
    jsonObjectMap.put(CLIENT_ID, userTaskInfo.clientIdentity());
    jsonObjectMap.put(START_MS, Long.toString(userTaskInfo.startMs()));
    jsonObjectMap.put(STATUS, status);
    jsonUserTaskList.add(jsonObjectMap);
  }

  private <T> Predicate<UserTaskManager.UserTaskInfo> checkInputFilter(Set<T> set) {
    if (set == null || set.isEmpty()) {
      return elem -> true;
    } else {
      return elem -> false;
    }
  }

  /**
   * We use userTasksIds, clientIds, endPoints, and types of User Task State to determine what UserTasks to add to the
   * result list. The ordering of the filters do not matter. We limit the returned result with entries so as to save
   * memory
   */
  private void populateFilteredTasks(List<UserTaskManager.UserTaskInfo> filteredTasks,
                                    List<UserTaskManager.UserTaskInfo> userTasks,
                                    CruiseControlParameters parameters,
                                    int entries) {
    if (filteredTasks.size() >= entries) {
      return;
    }
    Set<UUID> requestedUserTaskIds = ((UserTasksParameters) parameters).userTaskIds();
    Set<UserTaskManager.TaskState> requestedTaskStates = ((UserTasksParameters) parameters).types();
    Set<EndPoint> requestedEndPoints = ((UserTasksParameters) parameters).endPoints();
    Set<String> requestedClientIds = ((UserTasksParameters) parameters).clientIds();

    Consumer<UserTaskManager.UserTaskInfo> consumer = (elem) -> {
      if (filteredTasks.size() < entries) {
        filteredTasks.add(elem);
      }
    };

    // User LinkedList for better remove efficiency
    List<UserTaskManager.UserTaskInfo> tmpLinkedList = new LinkedList<>(userTasks);
    tmpLinkedList.stream()
                 .filter(checkInputFilter(requestedUserTaskIds).or(elem -> requestedUserTaskIds.contains(elem.userTaskId())))
                 .filter(checkInputFilter(requestedTaskStates).or(elem -> requestedTaskStates.contains(elem.state())))
                 .filter(checkInputFilter(requestedEndPoints).or(elem -> requestedEndPoints.contains(elem.endPoint())))
                 .filter(checkInputFilter(requestedClientIds).or(elem -> requestedClientIds.contains(elem.clientIdentity())))
                 .forEach(consumer);
  }

  private String getPlaintext(CruiseControlParameters parameters) {
    StringBuilder sb = new StringBuilder();
    int padding = 2;
    int userTaskIdLabelSize = 20;
    int clientAddressLabelSize = 20;
    int startMsLabelSize = 20;
    int statusLabelSize = 10;
    int requestURLLabelSize = 20;


    for (List<UserTaskManager.UserTaskInfo> taskList : _userTasksByTaskState.values()) {
      for (UserTaskManager.UserTaskInfo userTaskInfo : taskList) {
        userTaskIdLabelSize =
            userTaskIdLabelSize < userTaskInfo.userTaskId().toString().length() ? userTaskInfo.userTaskId()
                                                                                              .toString()
                                                                                              .length()
                                                                                : userTaskIdLabelSize;
        clientAddressLabelSize =
            clientAddressLabelSize < userTaskInfo.clientIdentity().length() ? userTaskInfo.clientIdentity().length()
                                                                            : clientAddressLabelSize;
        String dateFormatted = KafkaCruiseControlUtils.toDateString(userTaskInfo.startMs(), DATE_FORMAT, TIME_ZONE);
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
    for (UserTaskManager.UserTaskInfo userTaskInfo : prepareResultList(parameters)) {
      String dateFormatted = KafkaCruiseControlUtils.toDateString(userTaskInfo.startMs(), DATE_FORMAT, TIME_ZONE);
      sb.append(String.format(formattingStringBuilder.toString(), userTaskInfo.userTaskId().toString(), userTaskInfo.clientIdentity(),
                              dateFormatted, userTaskInfo.state(), userTaskInfo.requestWithParams())); // values
    }

    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJSONString(parameters) : getPlaintext(parameters);
    // Discard irrelevant response.
    _userTasksByTaskState.get(UserTaskManager.TaskState.ACTIVE).clear();
    _userTasksByTaskState.get(UserTaskManager.TaskState.COMPLETED).clear();
  }
}
