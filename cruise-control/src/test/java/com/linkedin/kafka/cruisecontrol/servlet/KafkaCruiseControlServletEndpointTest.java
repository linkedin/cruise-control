/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.response.UserTaskState;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import kafka.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;


public class KafkaCruiseControlServletEndpointTest {
  private static class MockResult implements CruiseControlResponse {
    public void discardIrrelevantResponse(CruiseControlParameters parameters) { }
    public void writeSuccessResponse(CruiseControlParameters parameters, HttpServletResponse response) throws
                                                                                                       IOException {
    }
  }

  @Test
  public void testUserTaskParameters() throws Exception, UnsupportedEncodingException {

    Time mockTime = new MockTime();
    UserTaskManager.UUIDGenerator mockUUIDGenerator = EasyMock.mock(UserTaskManager.UUIDGenerator.class);
    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);

    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(mockTime.milliseconds()).anyTimes();
    mockHttpSession.invalidate();
    mockHttpServletResponse.setHeader(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    UserTaskManager userTaskManager = new UserTaskManager(1000, 10, TimeUnit.HOURS.toMillis(6),
        100, mockTime, mockUUIDGenerator);

    // A hack to make 2 requests to same endpoint 'look' different to UserTaskManager
    Map<String,  String []> diffParam = new HashMap<>();
    diffParam.put("param", new String[]{"true"});

    // Creates 6 requests below. These will create 5 entries of UserTaskInfo (request 5 and 6 have same id, so will be 1 UserTaskInfo)
    HttpServletRequest mockHttpServletRequest1 =
        prepareTestRequest(mockHttpSession, null, "0.0.0.1", EndPoint.LOAD.toString(), Collections.emptyMap(), mockUUIDGenerator, false, "GET");
    HttpServletRequest mockHttpServletRequest2 =
        prepareTestRequest(mockHttpSession, null, "0.0.0.1", EndPoint.PROPOSALS.toString(), Collections.emptyMap(), mockUUIDGenerator, false, "GET");
    HttpServletRequest mockHttpServletRequest3 =
        prepareTestRequest(mockHttpSession, null, "0.0.0.2", EndPoint.REBALANCE.toString(), Collections.emptyMap(), mockUUIDGenerator, false, "POST");
    HttpServletRequest mockHttpServletRequest4 =
        prepareTestRequest(mockHttpSession, null, "0.0.0.3", EndPoint.REBALANCE.toString(), diffParam, mockUUIDGenerator, false, "POST");

    // Create user tasks with same user task ID. Used for get Task by Task ID test case.
    UUID userTaskId5 = UUID.randomUUID();
    HttpServletRequest mockHttpServletRequest5 =
        prepareTestRequest(mockHttpSession, userTaskId5, "0.0.0.1", EndPoint.REMOVE_BROKER.toString(), Collections.emptyMap(),
            mockUUIDGenerator, false, "POST");
    HttpServletRequest mockHttpServletRequest6 =
        prepareTestRequest(mockHttpSession, userTaskId5, "0.0.0.4", EndPoint.REMOVE_BROKER.toString(), Collections.emptyMap(), mockUUIDGenerator, true, "POST");

    EasyMock.replay(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);

    // Create 5 User Tasks. Note the 5th and 6th one have same user task id, thus count as 1.
    OperationFuture future1 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest1, mockHttpServletResponse, uuid -> new OperationFuture("future"), 0).get(0);

    userTaskManager.getOrCreateUserTask(mockHttpServletRequest2, mockHttpServletResponse, uuid -> new OperationFuture("future"), 0);

    userTaskManager.getOrCreateUserTask(mockHttpServletRequest3, mockHttpServletResponse, uuid -> new OperationFuture("future"), 0);

    userTaskManager.getOrCreateUserTask(mockHttpServletRequest4, mockHttpServletResponse, uuid -> new OperationFuture("future"), 0);

    OperationFuture future5 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest5, mockHttpServletResponse, uuid -> new OperationFuture("future"), 0).get(0);

    OperationFuture future6 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest6, mockHttpServletResponse, uuid -> new OperationFuture("future"), 1).get(1);

    UserTaskState userTaskState = new UserTaskState(userTaskManager.getActiveUserTasks(), userTaskManager.getCompletedUserTasks());

    // Test Case 1: Get all PROPOSAL or REBALANCE tasks
    Map<String,  String []> answerQueryParam1 = new HashMap<>();
    answerQueryParam1.put("param", new String[]{"true"});
    answerQueryParam1.put("endpoints", new String[]{EndPoint.PROPOSALS.toString() + "," + EndPoint.REBALANCE.toString()});
    HttpServletRequest answerQueryRequest1 = prepareRequest(mockHttpSession, null, "", EndPoint.USER_TASKS.toString(), answerQueryParam1, "GET");
    UserTasksParameters parameters1 = mockUserTasksParameters(answerQueryRequest1);
    List<UserTaskManager.UserTaskInfo> result1 = userTaskState.prepareResultList(parameters1);

    // Test Case 2: Get all tasks from client 0.0.0.1
    Map<String,  String []> answerQueryParam2 = new HashMap<>();
    answerQueryParam2.put("param", new String[]{"true"});
    answerQueryParam2.put("client_ids", new String[]{"0.0.0.1"});
    HttpServletRequest answerQueryRequest2 = prepareRequest(mockHttpSession, null, "", EndPoint.USER_TASKS.toString(), answerQueryParam2, "GET");
    UserTasksParameters parameters2 = mockUserTasksParameters(answerQueryRequest2);
    List<UserTaskManager.UserTaskInfo> result2 = userTaskState.prepareResultList(parameters2);

    // Test Case 3: Get all PROPOSALS and REMOVE_BROKERS from client 0.0.0.1
    Map<String,  String []> answerQueryParam3 = new HashMap<>();
    answerQueryParam3.put("param", new String[]{"true"});
    answerQueryParam3.put("client_ids", new String[]{"0.0.0.1"});
    answerQueryParam3.put("endpoints", new String[]{EndPoint.PROPOSALS.toString() + "," + EndPoint.REMOVE_BROKER.toString()});
    HttpServletRequest answerQueryRequest3 = prepareRequest(mockHttpSession, null, "", EndPoint.USER_TASKS.toString(), answerQueryParam3, "GET");
    UserTasksParameters parameters3 = mockUserTasksParameters(answerQueryRequest3);
    List<UserTaskManager.UserTaskInfo> result3 = userTaskState.prepareResultList(parameters3);

    // Test Case 4: Get all tasks limit to 4 entries
    Map<String,  String []> answerQueryParam4 = new HashMap<>();
    answerQueryParam4.put("param", new String[]{"true"});
    answerQueryParam4.put("entries", new String[]{"4"});
    HttpServletRequest answerQueryRequest4 = prepareRequest(mockHttpSession, null, "", EndPoint.USER_TASKS.toString(), answerQueryParam4, "GET");
    UserTasksParameters parameters4 = mockUserTasksParameters(answerQueryRequest4);
    List<UserTaskManager.UserTaskInfo> result4 = userTaskState.prepareResultList(parameters4);


    Assert.assertEquals(3, result1.size());         // Test Case 1 result
    EasyMock.reset(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);
    Assert.assertEquals(3, result2.size());         // Test Case 2 result
    EasyMock.reset(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);
    Assert.assertEquals(2, result3.size());         // Test Case 3 result
    EasyMock.reset(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);
    Assert.assertEquals(4, result4.size());         // Test Case 4 result
    EasyMock.reset(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);


    // Resolve futures. Allow those tasks to be moved into completed state
    future1.complete(new MockResult());
    future5.complete(new MockResult());
    future6.complete(new MockResult());

    // Update task manager active vs completed state
    userTaskManager.updateTaskState();

    // Now the UserTaskManager state has changed, so we reload the states
    UserTaskState userTaskState2 = new UserTaskState(userTaskManager.getActiveUserTasks(), userTaskManager.getCompletedUserTasks());

    // Test Case 5: Get all LOAD or REMOVE_BROKER tasks that's completed and with user task id userTaskId5
    Map<String,  String []> answerQueryParam5 = new HashMap<>();
    answerQueryParam5.put("param", new String[]{"true"});
    answerQueryParam5.put("endpoints", new String[]{EndPoint.LOAD.toString() + "," + EndPoint.REMOVE_BROKER.toString()});
    answerQueryParam5.put("user_task_ids", new String[]{userTaskId5.toString()});
    answerQueryParam5.put("types", new String[]{UserTaskManager.TaskState.COMPLETED.toString()});

    HttpServletRequest answerQueryRequest5 = prepareRequest(mockHttpSession, null, "", EndPoint.USER_TASKS.toString(), answerQueryParam5, "GET");
    UserTasksParameters parameters5 = mockUserTasksParameters(answerQueryRequest5);
    List<UserTaskManager.UserTaskInfo> result5 = userTaskState2.prepareResultList(parameters5);

    Assert.assertEquals(1, result5.size());         // Test Case 5 result
    EasyMock.reset(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);

  }

  // Some how we cannot instantiate UserTasksParameters (fail at instantiating LOGGER object), so we mock it.
  private UserTasksParameters mockUserTasksParameters(HttpServletRequest answerQueryRequest) throws UnsupportedEncodingException {
    UserTasksParameters parameters = EasyMock.mock(UserTasksParameters.class);
    EasyMock.expect(parameters.userTaskIds()).andReturn(ParameterUtils.userTaskIds(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.clientIds()).andReturn(ParameterUtils.userTaskClientIds(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.endPoints()).andReturn(ParameterUtils.userTaskEndPoints(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.endPoint()).andReturn(ParameterUtils.endPoint(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.taskStates()).andReturn(ParameterUtils.userTaskState(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.listLength()).andReturn(ParameterUtils.entries(answerQueryRequest)).anyTimes();

    EasyMock.replay(parameters);
    return parameters;
  }

  private HttpServletRequest prepareTestRequest(HttpSession session, UUID userTaskId, String clientId, String resource,
      Map<String, String []> params, UserTaskManager.UUIDGenerator mockUUIDGenerator, boolean addToRequest, String method) {

    UUID uuidForGenerator = (userTaskId == null ? UUID.randomUUID() : userTaskId);
    String uuidForRequest = ((addToRequest && userTaskId != null) ? userTaskId.toString() : null);

    EasyMock.expect(mockUUIDGenerator.randomUUID()).andReturn(uuidForGenerator).once();
    return prepareRequest(session, uuidForRequest, clientId, resource, params, method);
  }

  private HttpServletRequest prepareRequest(HttpSession session, String userTaskId, String clientId, String resource,
      Map<String, String []> params, String method) {
    HttpServletRequest request = EasyMock.mock(HttpServletRequest.class);

    EasyMock.expect(request.getSession()).andReturn(session).anyTimes();
    EasyMock.expect(request.getSession(false)).andReturn(session).anyTimes();
    EasyMock.expect(request.getMethod()).andReturn(method).anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn(KafkaCruiseControlServletUtils.REQUEST_URI + resource).anyTimes();
    EasyMock.expect(request.getParameterMap()).andReturn(params).anyTimes();
    EasyMock.expect(request.getHeader(UserTaskManager.USER_TASK_HEADER_NAME)).andReturn(userTaskId).anyTimes();
    EasyMock.expect(request.getRemoteHost()).andReturn("test-host").anyTimes();
    for (String headerName : KafkaCruiseControlServletUtils.HEADERS_TO_TRY) {
      EasyMock.expect(request.getHeader(headerName)).andReturn(clientId).anyTimes();
    }

    for (String param : ParameterUtils.PARAMS_TO_GET) {
      String result = null;
      // Assume all parameters stored in first array entry
      if (params.get(param) != null) {
        result = params.get(param)[0];
      }
      EasyMock.expect(request.getParameter(param)).andReturn(result).anyTimes();
    }

    EasyMock.replay(request);

    return request;
  }
}
