/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.DATE_FORMAT;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.TIME_ZONE;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;
import static java.lang.Math.max;


public class UserTaskState extends AbstractCruiseControlResponse {
  protected static final String USER_TASK_ID = "UserTaskId";
  protected static final String REQUEST_URL = "RequestURL";
  protected static final String CLIENT_ID = "ClientIdentity";
  protected static final String START_MS = "StartMs";
  protected static final String STATUS = "Status";
  protected static final String USER_TASKS = "userTasks";
  protected static final String ORIGINAL_RESPONSE = "originalResponse";
  protected final List<UserTaskManager.UserTaskInfo> _userTasks;

  public UserTaskState(List<UserTaskManager.UserTaskInfo> userTasks, KafkaCruiseControlConfig config) {
    super(config);
    _userTasks = userTasks;
  }

  protected String getJSONString(UserTasksParameters parameters) {
    List<Map<String, Object>> jsonUserTaskList = new ArrayList<>();
    for (UserTaskManager.UserTaskInfo taskInfo : prepareResultList(parameters)) {
      addJSONTask(jsonUserTaskList,
                  taskInfo,
                  parameters.fetchCompletedTask() && (taskInfo.state() != UserTaskManager.TaskState.ACTIVE));
    }
    Map<String, Object> jsonResponse = new HashMap<>();
    jsonResponse.put(USER_TASKS, jsonUserTaskList);
    jsonResponse.put(VERSION, JSON_VERSION);
    return new Gson().toJson(jsonResponse);
  }

  /**
   * Prepare the result list as a list of user task info.
   *
   * @param parameters User task parameters.
   * @return The result list as a list of user task info.
   */
  public List<UserTaskManager.UserTaskInfo> prepareResultList(UserTasksParameters parameters) {
    int entries = parameters.entries();
    // If entries argument isn't given in request, we give MAX_VALUE to entries. Thus need to avoid instantiating
    // to MAX_VALUE
    List<UserTaskManager.UserTaskInfo> resultList = (entries == Integer.MAX_VALUE ? new ArrayList<>() : new ArrayList<>(entries));

    populateFilteredTasks(resultList, _userTasks, parameters, entries);
    return resultList.subList(0, Math.min(entries, resultList.size()));
  }

  protected void addJSONTask(List<Map<String, Object>> jsonUserTaskList,
                           UserTaskManager.UserTaskInfo userTaskInfo,
                           boolean fetchCompletedTask) {
    Map<String, Object> jsonObjectMap = new HashMap<>(fetchCompletedTask ? 6 : 5);
    String status = userTaskInfo.state().toString();
    jsonObjectMap.put(USER_TASK_ID, userTaskInfo.userTaskId().toString());
    jsonObjectMap.put(REQUEST_URL, userTaskInfo.requestWithParams());
    jsonObjectMap.put(CLIENT_ID, userTaskInfo.clientIdentity());
    jsonObjectMap.put(START_MS, Long.toString(userTaskInfo.startMs()));
    jsonObjectMap.put(STATUS, status);
    // Populate original response of completed task if requested so.
    if (fetchCompletedTask) {
      jsonObjectMap.put(ORIGINAL_RESPONSE, completedTaskResponse(userTaskInfo));
    }
    jsonUserTaskList.add(jsonObjectMap);
  }

  protected static <T> Predicate<UserTaskManager.UserTaskInfo> checkInputFilter(Set<T> set) {
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
  protected static void populateFilteredTasks(List<UserTaskManager.UserTaskInfo> filteredTasks,
                                            List<UserTaskManager.UserTaskInfo> userTasks,
                                            UserTasksParameters parameters,
                                            int entries) {
    if (filteredTasks.size() >= entries) {
      return;
    }
    Set<UUID> requestedUserTaskIds = parameters.userTaskIds();
    Set<UserTaskManager.TaskState> requestedTaskStates = parameters.types();
    Set<CruiseControlEndPoint> requestedEndPoints = parameters.endPoints();
    Set<String> requestedClientIds = parameters.clientIds();

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

  protected String getPlaintext(UserTasksParameters parameters) {
    StringBuilder sb = new StringBuilder();
    int padding = 2;
    int userTaskIdLabelSize = 20;
    int clientAddressLabelSize = 20;
    int startMsLabelSize = 20;
    int statusLabelSize = 10;
    int requestURLLabelSize = 20;

    List<UserTaskManager.UserTaskInfo> taskInfoList = prepareResultList(parameters);
    for (UserTaskManager.UserTaskInfo userTaskInfo : taskInfoList) {
      userTaskIdLabelSize = max(userTaskIdLabelSize, userTaskInfo.userTaskId().toString().length());
      clientAddressLabelSize = max(clientAddressLabelSize, userTaskInfo.clientIdentity().length());
      String dateFormatted = KafkaCruiseControlUtils.toDateString(userTaskInfo.startMs(), DATE_FORMAT, TIME_ZONE);
      startMsLabelSize = max(startMsLabelSize, dateFormatted.length());
      statusLabelSize = max(statusLabelSize, userTaskInfo.state().toString().length());
      requestURLLabelSize = max(requestURLLabelSize, userTaskInfo.requestWithParams().length());
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
    for (UserTaskManager.UserTaskInfo userTaskInfo : taskInfoList) {
      String dateFormatted = KafkaCruiseControlUtils.toDateString(userTaskInfo.startMs(), DATE_FORMAT, TIME_ZONE);
      sb.append(String.format(formattingStringBuilder.toString(), userTaskInfo.userTaskId().toString(), userTaskInfo.clientIdentity(),
                              dateFormatted, userTaskInfo.state(), userTaskInfo.requestWithParams())); // values
    }

    // Populate original response of completed tasks if requested so.
    if (parameters.fetchCompletedTask()) {
      for (UserTaskManager.UserTaskInfo userTaskInfo : taskInfoList) {
        if (userTaskInfo.state() == UserTaskManager.TaskState.ACTIVE) {
          continue;
        }
        sb.append("\nOriginal response for task ")
          .append(userTaskInfo.userTaskId())
          .append(":\n")
          .append(completedTaskResponse(userTaskInfo));
      }
    }

    return sb.toString();
  }

  protected static String completedTaskResponse(UserTaskManager.UserTaskInfo userTaskInfo) {
    try {
      CruiseControlResponse response = userTaskInfo.futures().get(userTaskInfo.futures().size() - 1).get();
      return response.cachedResponse();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("Error happened in fetching response for task " + userTaskInfo.userTaskId().toString(), e);
    }
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    UserTasksParameters userTasksParameters = (UserTasksParameters) parameters;
    _cachedResponse = userTasksParameters.json() ? getJSONString(userTasksParameters) :
                                                   getPlaintext(userTasksParameters);
    // Discard irrelevant response.
    _userTasks.clear();
  }
}
