/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import kafka.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;


public class UserTaskManagerTest {
  @Test
  public void testCreateUserTask() {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UUIDGenerator mockUUIDGenerator = EasyMock.mock(UserTaskManager.UUIDGenerator.class);
    EasyMock.expect(mockUUIDGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn((long) 100).anyTimes();

    HttpServletRequest mockHttpServletRequest1 = prepareRequest(mockHttpSession, null);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    Capture<String> userTaskHeader = Capture.newInstance();
    Capture<String> userTaskHeaderValue = Capture.newInstance();
    mockHttpServletResponse.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);

    OperationFuture<Integer> future = new OperationFuture<>("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, new MockTime(), mockUUIDGenerator);
    // test-case: create user-task based on request and get future
    OperationFuture future1 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest1, mockHttpServletResponse, () -> future, 0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future1);

    EasyMock.reset(mockHttpServletResponse);
    // test-case: get same future back using sessions
    OperationFuture future2 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest1, mockHttpServletResponse, () -> future, 0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future2);

    HttpServletRequest mockHttpServletRequest2 = prepareRequest(mockHttpSession, testUserTaskId.toString());
    EasyMock.reset(mockHttpServletResponse);
    // test-case: get future back using user-task-id
    OperationFuture future3 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest2, mockHttpServletResponse, () -> future, 0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future3);

    userTaskManager.close();
  }

  @Test
  public void testAddStepsFutures() {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UUIDGenerator mockUUIDGenerator = EasyMock.mock(UserTaskManager.UUIDGenerator.class);
    EasyMock.expect(mockUUIDGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn((long) 100).anyTimes();

    HttpServletRequest mockHttpServletRequest = prepareRequest(mockHttpSession, null);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    mockHttpServletResponse.setHeader(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.replay(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);

    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, new MockTime(), mockUUIDGenerator);

    OperationFuture<Integer> testFuture1 = new OperationFuture<>("testFuture1");
    OperationFuture<Integer> testFuture2 = new OperationFuture<>("testFuture2");

    OperationFuture insertedFuture1 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest, mockHttpServletResponse, () -> testFuture1, 0);
    Assert.assertEquals(testFuture1, insertedFuture1);
    EasyMock.reset(mockHttpServletResponse);
    OperationFuture insertedFuture2 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest, mockHttpServletResponse, () -> testFuture2, 1);
    Assert.assertEquals(testFuture2, insertedFuture2);

    Assert.assertEquals(userTaskManager.getFuturesByUserTaskId(testUserTaskId, mockHttpServletRequest).size(), 2);
    userTaskManager.close();
  }

  @Test
  public void testCompletedTasks() throws Exception {
    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn((long) 100).anyTimes();
    mockHttpSession.invalidate();

    HttpServletRequest mockHttpServletRequest = prepareRequest(mockHttpSession, null);
    UserTaskManager.UUIDGenerator mockUUIDGenerator = EasyMock.mock(UserTaskManager.UUIDGenerator.class);
    EasyMock.expect(mockUUIDGenerator.randomUUID()).andReturn(UUID.randomUUID()).anyTimes();

    OperationFuture<Integer> future = new OperationFuture<>("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, new MockTime(), mockUUIDGenerator);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    Capture<String> userTaskHeader = Capture.newInstance();
    Capture<String> userTaskHeaderValue = Capture.newInstance();
    mockHttpServletResponse.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);
    // test-case: verify if the background cleaner task removes tasks that are completed
    OperationFuture future1 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest, mockHttpServletResponse, () -> future, 0);
    Assert.assertEquals(future, future1);

    future1.cancel(true);
    Thread.sleep(TimeUnit.SECONDS.toMillis(UserTaskManager.USER_TASK_SCANNER_PERIOD_SECONDS * 4));

    Assert.assertTrue(future.isDone());
    Assert.assertTrue(future.isCancelled());

    userTaskManager.close();
  }

  @Test
  public void testExpireSession() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UUIDGenerator mockUUIDGenerator = EasyMock.mock(UserTaskManager.UUIDGenerator.class);
    EasyMock.expect(mockUUIDGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    Time mockTime = new MockTime();
    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(mockTime.milliseconds()).anyTimes();
    mockHttpSession.invalidate();

    HttpServletRequest mockHttpServletRequest = prepareRequest(mockHttpSession, null);

    OperationFuture<Integer> future = new OperationFuture<>("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, mockTime, mockUUIDGenerator);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    mockHttpServletResponse.setHeader(EasyMock.anyString(), EasyMock.anyString());

    EasyMock.replay(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);
    // test-case: test if the sessions are removed on expiration
    OperationFuture future1 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest, mockHttpServletResponse, () -> future, 0);
    Assert.assertEquals(future, future1);

    mockTime.sleep(1001);
    Thread.sleep(TimeUnit.SECONDS.toMillis(UserTaskManager.USER_TASK_SCANNER_PERIOD_SECONDS + 1));

    OperationFuture future2 = userTaskManager.getFuture(mockHttpServletRequest);
    Assert.assertNull(future2);

    userTaskManager.close();
  }

  @Test
  public void testCloseSession() {
    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    mockHttpSession.invalidate();
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn((long) 100).anyTimes();

    UserTaskManager.UUIDGenerator mockUUIDGenerator = EasyMock.mock(UserTaskManager.UUIDGenerator.class);
    EasyMock.expect(mockUUIDGenerator.randomUUID()).andReturn(UUID.randomUUID()).anyTimes();

    HttpServletRequest mockHttpServletRequest = prepareRequest(mockHttpSession, null);

    OperationFuture<Integer> future = new OperationFuture<>("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, new MockTime(), mockUUIDGenerator);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    mockHttpServletResponse.setHeader(EasyMock.anyString(), EasyMock.anyString());

    EasyMock.replay(mockUUIDGenerator, mockHttpSession, mockHttpServletResponse);
    // test-case: close session invalidates session
    OperationFuture future1 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest, mockHttpServletResponse, () -> future, 0);
    Assert.assertEquals(future, future1);

    userTaskManager.closeSession(mockHttpServletRequest);

    OperationFuture future2 = userTaskManager.getFuture(mockHttpServletRequest);
    Assert.assertNull(future2);

    userTaskManager.close();
  }

  @Test
  public void testMaximumActiveTasks() {
    HttpSession mockHttpSession1 = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession1.getLastAccessedTime()).andReturn((long) 100).anyTimes();

    HttpServletRequest mockHttpServletRequest1 = prepareRequest(mockHttpSession1, null);

    OperationFuture<Integer> future = new OperationFuture<>("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, new MockTime());

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    mockHttpServletResponse.setHeader(EasyMock.anyString(), EasyMock.anyString());

    EasyMock.replay(mockHttpSession1, mockHttpServletResponse);
    // test-case: test max limitation active tasks
    OperationFuture future1 =
        userTaskManager.getOrCreateUserTask(mockHttpServletRequest1, mockHttpServletResponse, () -> future, 0);
    Assert.assertEquals(future, future1);

    HttpSession mockHttpSession2 = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession2.getLastAccessedTime()).andReturn((long) 100).anyTimes();
    EasyMock.replay(mockHttpSession2);
    EasyMock.reset(mockHttpServletResponse);

    HttpServletRequest mockHttpServletRequest2 = prepareRequest(mockHttpSession2, null, "/test2");
    try {
      OperationFuture future2 =
          userTaskManager.getOrCreateUserTask(mockHttpServletRequest2, mockHttpServletResponse, () -> future, 0);
      Assert.assertEquals(future, future2);
    } catch (RuntimeException e) {
      userTaskManager.close();
      return;
    }
    Assert.fail("Don't expect to be here!");
  }

  private HttpServletRequest prepareRequest(HttpSession session, String userTaskId) {
    return prepareRequest(session, userTaskId, "/test");
  }

  private HttpServletRequest prepareRequest(HttpSession session, String userTaskId, String resource) {
    HttpServletRequest request = EasyMock.mock(HttpServletRequest.class);

    EasyMock.expect(request.getSession()).andReturn(session).anyTimes();
    EasyMock.expect(request.getSession(false)).andReturn(session).anyTimes();
    EasyMock.expect(request.getMethod()).andReturn("GET").anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn(resource).anyTimes();
    EasyMock.expect(request.getParameterMap()).andReturn(Collections.emptyMap()).anyTimes();
    EasyMock.expect(request.getHeader(UserTaskManager.USER_TASK_HEADER_NAME)).andReturn(userTaskId).anyTimes();
    for (String headerName : KafkaCruiseControlServletUtils.HEADERS_TO_TRY) {
      EasyMock.expect(request.getHeader(headerName)).andReturn("localhost").anyTimes();
    }

    EasyMock.replay(request);

    return request;
  }
}
