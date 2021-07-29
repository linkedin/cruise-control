/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.vertx.VertxRequestContext;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.Http1xServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.Session;
import kafka.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.GET_METHOD;

public class UserTaskManagerTest {

  @Ignore
  @Test
  public void testCreateUserTask() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(100L).anyTimes();

    HttpServletRequest mockHttpServletRequest1 = prepareServletRequest(mockHttpSession, null);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    Capture<String> userTaskHeader = Capture.newInstance();
    Capture<String> userTaskHeaderValue = Capture.newInstance();
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext handler1 = new ServletRequestContext(mockHttpServletRequest1, mockHttpServletResponse, cruiseControlConfigMock);
    handler1.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
            100, new MockTime(), mockUuidGenerator);
    // test-case: create user-task based on request and get future
    OperationFuture future1 =
            userTaskManager.getOrCreateUserTask(handler1, uuid -> future, 0, true, null).get(0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future1);
    EasyMock.verify(mockUuidGenerator, mockHttpServletResponse, mockHttpSession);
    EasyMock.reset(mockHttpServletResponse);
    // test-case: get same future back using sessions
    OperationFuture future2 =
            userTaskManager.getOrCreateUserTask(handler1, uuid -> future, 0, true, null).get(0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future2);

    HttpServletRequest mockHttpServletRequest2 = prepareServletRequest(mockHttpSession, testUserTaskId.toString());
    EasyMock.reset(mockHttpServletResponse);
    // test-case: get future back using user-task-id
    ServletRequestContext handler2 = new ServletRequestContext(mockHttpServletRequest2, mockHttpServletResponse, cruiseControlConfigMock);
    OperationFuture future3 =
            userTaskManager.getOrCreateUserTask(handler2, uuid -> future, 0, true, null).get(0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future3);

    EasyMock.reset(mockHttpServletResponse);

    // test-case: for sync task, UserTaskManager does not create mapping between request URL and UUID.
    HttpServletRequest mockHttpServletRequest3 = prepareServletRequest(null, null, "test_sync_request", Collections.emptyMap());
    ServletRequestContext handler3 = new ServletRequestContext(mockHttpServletRequest3, mockHttpServletResponse, cruiseControlConfigMock);
    OperationFuture future4 =
            userTaskManager.getOrCreateUserTask(handler3, uuid -> future, 0, false, null).get(0);

    // New async request have session mapping where a session key is mapped to the UUID associated with the request. Such a mapping is
    // not created for sync request. So in the 2 asserts below , we expect that given a http request, we are able to find request associated UUID
    // for async request but not for sync request.
    UUID savedUUID = userTaskManager.getUserTaskId(handler3);
    Assert.assertNull(savedUUID);
    Assert.assertEquals(future4, future);

    EasyMock.reset(mockHttpServletResponse);
    OperationFuture future5 =
            userTaskManager.getOrCreateUserTask(handler3, uuid -> future, 0, true, null).get(0);
    savedUUID = userTaskManager.getUserTaskId(handler3);
    Assert.assertNotEquals(savedUUID, null);
    Assert.assertEquals(future5, future);

    userTaskManager.close();
  }

  @Ignore
  @Test
  public void testServletSessionsShareUserTask() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(100L).anyTimes();

    Map<String, String []> requestParams1 = new HashMap<>();
    requestParams1.put("param", new String[]{"true"});
    HttpServletRequest mockHttpServletRequest1 = prepareServletRequest(mockHttpSession, null, "test", requestParams1);
    HttpServletResponse mockHttpServletResponse1 = EasyMock.mock(HttpServletResponse.class);
    Capture<String> userTaskHeader = Capture.newInstance();
    Capture<String> userTaskHeaderValue = Capture.newInstance();
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext handler1 = new ServletRequestContext(mockHttpServletRequest1, mockHttpServletResponse1, cruiseControlConfigMock);
    handler1.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    Map<String, String []> requestParams2 = new HashMap<>();
    requestParams2.put("param", new String[]{"true"});
    HttpServletRequest mockHttpServletRequest2 = prepareServletRequest(mockHttpSession, null, "test", requestParams2);
    HttpServletResponse mockHttpServletResponse2 = EasyMock.mock(HttpServletResponse.class);
    ServletRequestContext handler2 = new ServletRequestContext(mockHttpServletRequest2, mockHttpServletResponse2, cruiseControlConfigMock);
    handler2.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    Map<String, String []> requestParams3 = new HashMap<>();
    requestParams3.put("param", new String[]{"true"});
    HttpServletRequest mockHttpServletRequest3 = prepareServletRequest(mockHttpSession, testUserTaskId.toString(), "test", requestParams3);
    HttpServletResponse mockHttpServletResponse3 = EasyMock.mock(HttpServletResponse.class);
    ServletRequestContext handler3 = new ServletRequestContext(mockHttpServletRequest3, mockHttpServletResponse3, cruiseControlConfigMock);
    handler3.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse1, mockHttpServletResponse2, mockHttpServletResponse3);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 5, TimeUnit.HOURS.toMillis(6),
            100, new MockTime(), mockUuidGenerator);
    userTaskManager.getOrCreateUserTask(handler1, uuid -> future, 0, true, null);
    userTaskManager.getOrCreateUserTask(handler2, uuid -> future, 0, true, null);
    // Test UserTaskManger can recognize the previous created task by taskId.
    userTaskManager.getOrCreateUserTask(handler3, uuid -> future, 0, true, null);

    // The 2nd request should reuse the UserTask created for the 1st request since they use the same session and send the same request.
    Assert.assertEquals(1, userTaskManager.numActiveSessionKeys());
  }

  @Test
  public void testVertexSessionsShareUserTask() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    Session mockHttpSession = EasyMock.mock(Session.class);
    EasyMock.expect(mockHttpSession.lastAccessed()).andReturn(100L).anyTimes();

    Map<String, String []> requestParams1 = new HashMap<>();
    requestParams1.put("param", new String[]{"true"});
    Http1xServerResponse response = EasyMock.niceMock(Http1xServerResponse.class);
    RoutingContext context = prepareVertxRequest(mockHttpSession, null, "test", requestParams1, response);
    //    Capture<String> userTaskHeader = Capture.newInstance();
    //    Capture<String> userTaskHeaderValue = Capture.newInstance();
    VertxRequestContext handler1 = new VertxRequestContext(context, EasyMock.mock(KafkaCruiseControlConfig.class));
    EasyMock.expect(response.putHeader(EasyMock.anyString(),
            EasyMock.anyString())).andReturn(EasyMock.mock(Http1xServerResponse.class)).anyTimes();
    //response.putHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    Map<String, String []> requestParams2 = new HashMap<>();
    requestParams2.put("param", new String[]{"true"});
    Http1xServerResponse response2 = EasyMock.mock(Http1xServerResponse.class);
    RoutingContext context2 = prepareVertxRequest(mockHttpSession, null, "test", requestParams2, response2);
    VertxRequestContext handler2 = new VertxRequestContext(context2, EasyMock.mock(KafkaCruiseControlConfig.class));
    EasyMock.expect(response2.putHeader(EasyMock.anyString(),
            EasyMock.anyString())).andReturn(EasyMock.mock(Http1xServerResponse.class)).anyTimes();
    //handler2.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    Map<String, String []> requestParams3 = new HashMap<>();
    requestParams3.put("param", new String[]{"true"});
    Http1xServerResponse response3 = EasyMock.mock(Http1xServerResponse.class);
    RoutingContext context3 = prepareVertxRequest(mockHttpSession, testUserTaskId.toString(), "test", requestParams3, response3);
    VertxRequestContext handler3 = new VertxRequestContext(context3, EasyMock.mock(KafkaCruiseControlConfig.class));
    EasyMock.expect(response3.putHeader(EasyMock.anyString(),
            EasyMock.anyString())).andReturn(EasyMock.mock(Http1xServerResponse.class)).anyTimes();
    //handler3.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUuidGenerator, mockHttpSession, response, response2, response3);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 5, TimeUnit.HOURS.toMillis(6),
            100, new MockTime(), mockUuidGenerator);
    userTaskManager.getOrCreateUserTask(handler1, uuid -> future, 0, true, null);
    userTaskManager.getOrCreateUserTask(handler2, uuid -> future, 0, true, null);
    // Test UserTaskManger can recognize the previous created task by taskId.
    userTaskManager.getOrCreateUserTask(handler3, uuid -> future, 0, true, null);

    // The 2nd request should reuse the UserTask created for the 1st request since they use the same session and send the same request.
    Assert.assertEquals(1, userTaskManager.numActiveSessionKeys());
    userTaskManager.close();
  }

  @Ignore
  @Test
  public void testSevletAddStepsFutures() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    // Change mock session's last access time to always return current time to avoid unintended recycling of session.
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(System.currentTimeMillis()).anyTimes();

    HttpServletRequest mockHttpServletRequest = prepareServletRequest(mockHttpSession, null);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext handler = new ServletRequestContext(mockHttpServletRequest, mockHttpServletResponse, cruiseControlConfigMock);
    handler.setHeader(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);

    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
            100, new MockTime(), mockUuidGenerator);

    OperationFuture testFuture1 = new OperationFuture("testFuture1");
    OperationFuture testFuture2 = new OperationFuture("testFuture2");

    OperationFuture insertedFuture1 =
            userTaskManager.getOrCreateUserTask(handler, uuid -> testFuture1, 0,
                    true, null).get(0);
    Assert.assertEquals(testFuture1, insertedFuture1);
    EasyMock.reset(mockHttpServletResponse);
    OperationFuture insertedFuture2 =
            userTaskManager.getOrCreateUserTask(handler, uuid -> testFuture2, 1,
                    true, null).get(1);
    Assert.assertEquals(testFuture2, insertedFuture2);

    Assert.assertEquals(userTaskManager.getUserTaskByUserTaskId(testUserTaskId, handler).futures().size(), 2);
    userTaskManager.close();
  }

  @Test
  public void testVertxAddStepsFutures() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    // Change mock session's last access time to always return current time to avoid unintended recycling of session.
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(System.currentTimeMillis()).anyTimes();

    HttpServletRequest mockHttpServletRequest = prepareServletRequest(mockHttpSession, null);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext handler = new ServletRequestContext(mockHttpServletRequest, mockHttpServletResponse, cruiseControlConfigMock);
    handler.setHeader(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);

    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
            100, new MockTime(), mockUuidGenerator);

    OperationFuture testFuture1 = new OperationFuture("testFuture1");
    OperationFuture testFuture2 = new OperationFuture("testFuture2");

    OperationFuture insertedFuture1 =
            userTaskManager.getOrCreateUserTask(handler, uuid -> testFuture1, 0,
                    true, null).get(0);
    Assert.assertEquals(testFuture1, insertedFuture1);
    EasyMock.verify(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);
    EasyMock.reset(mockHttpServletResponse);
    OperationFuture insertedFuture2 =
            userTaskManager.getOrCreateUserTask(handler, uuid -> testFuture2, 1,
                    true, null).get(1);
    Assert.assertEquals(testFuture2, insertedFuture2);

    Assert.assertEquals(userTaskManager.getUserTaskByUserTaskId(testUserTaskId, handler).futures().size(), 2);
    userTaskManager.close();
  }

  @Ignore
  @Test
  public void testCompletedTasks() throws Exception {
    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(100L).anyTimes();
    mockHttpSession.invalidate();

    HttpServletRequest mockHttpServletRequest = prepareServletRequest(mockHttpSession, null);
    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(UUID.randomUUID()).anyTimes();

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
            100, new MockTime(), mockUuidGenerator);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext handler = new ServletRequestContext(mockHttpServletRequest, mockHttpServletResponse, cruiseControlConfigMock);
    Capture<String> userTaskHeader = Capture.newInstance();
    Capture<String> userTaskHeaderValue = Capture.newInstance();
    handler.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);
    // test-case: verify if the background cleaner task removes tasks that are completed
    OperationFuture future1 =
            userTaskManager.getOrCreateUserTask(handler, uuid -> future, 0, true, null).get(0);
    Assert.assertEquals(future, future1);

    future1.cancel(true);
    Thread.sleep(TimeUnit.SECONDS.toMillis(UserTaskManager.USER_TASK_SCANNER_PERIOD_SECONDS * 4));

    Assert.assertTrue(future.isDone());
    Assert.assertTrue(future.isCancelled());
    EasyMock.verify(mockHttpSession, mockUuidGenerator, mockHttpServletResponse);
    userTaskManager.close();
  }

  @Ignore
  @Test
  public void testServletExpireSession() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    Time mockTime = new MockTime();
    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(mockTime.milliseconds()).anyTimes();
    mockHttpSession.invalidate();
    HttpServletRequest mockHttpServletRequest = prepareServletRequest(mockHttpSession, null);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
            100, mockTime, mockUuidGenerator);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext servletHandler = new ServletRequestContext(mockHttpServletRequest, mockHttpServletResponse, cruiseControlConfigMock);
    servletHandler.setHeader(EasyMock.anyString(), EasyMock.anyString());

    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);
    // test-case: test if the sessions are removed on expiration
    OperationFuture future1 =
            userTaskManager.getOrCreateUserTask(servletHandler, uuid -> future, 0, true, null).get(0);
    Assert.assertEquals(future, future1);

    mockTime.sleep(1001);
    Thread.sleep(TimeUnit.SECONDS.toMillis(UserTaskManager.USER_TASK_SCANNER_PERIOD_SECONDS + 1));

    OperationFuture servletFuture2 = userTaskManager.getFuture(servletHandler);
    Assert.assertNull(servletFuture2);

    userTaskManager.close();
  }

  @Ignore
  @Test
  public void testVertxExpireSession() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    Time mockTime = new MockTime();
    Session vertxSession = EasyMock.mock(Session.class);
    EasyMock.expect(vertxSession.lastAccessed()).andReturn(mockTime.milliseconds()).anyTimes();
    vertxSession.destroy();
    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    Http1xServerResponse mockVertxResponse = EasyMock.mock(Http1xServerResponse.class);
    RoutingContext mockContext = prepareVertxRequest(vertxSession, null, mockVertxResponse);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
            100, mockTime, mockUuidGenerator);

    VertxRequestContext vertxHandler = new VertxRequestContext(mockContext, EasyMock.mock(KafkaCruiseControlConfig.class));
    EasyMock.expect(mockVertxResponse.putHeader(UserTaskManager.USER_TASK_HEADER_NAME,
            testUserTaskId.toString())).andReturn(mockVertxResponse).anyTimes();

    EasyMock.replay(mockUuidGenerator, mockHttpServletResponse, mockVertxResponse);
    // test-case: test if the sessions are removed on expiration

    OperationFuture furure1 =
            userTaskManager.getOrCreateUserTask(vertxHandler, uuid -> future, 0, true, null).get(0);
    Assert.assertEquals(future, furure1);

    mockTime.sleep(1001);
    Thread.sleep(TimeUnit.SECONDS.toMillis(UserTaskManager.USER_TASK_SCANNER_PERIOD_SECONDS + 1));

    OperationFuture future2 = userTaskManager.getFuture(vertxHandler);
    Assert.assertNull(future2);
    EasyMock.verify(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);
    userTaskManager.close();
  }

  @Ignore
  @Test
  public void testMaximumActiveTasks() throws Exception {
    HttpSession mockHttpSession1 = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession1.getLastAccessedTime()).andReturn(100L).anyTimes();

    HttpServletRequest mockHttpServletRequest1 = prepareServletRequest(mockHttpSession1, null);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
            100, new MockTime());

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext handler1 = new ServletRequestContext(mockHttpServletRequest1, mockHttpServletResponse, cruiseControlConfigMock);
    handler1.setHeader(EasyMock.anyString(), EasyMock.anyString());

    EasyMock.replay(mockHttpSession1, mockHttpServletResponse);
    // test-case: test max limitation active tasks
    OperationFuture future1 =
            userTaskManager.getOrCreateUserTask(handler1, uuid -> future, 0, true, null).get(0);
    Assert.assertEquals(future, future1);
    EasyMock.verify(mockHttpSession1, mockHttpServletRequest1, mockHttpServletResponse);
    HttpSession mockHttpSession2 = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession2.getLastAccessedTime()).andReturn(100L).anyTimes();
    EasyMock.replay(mockHttpSession2);
    EasyMock.reset(mockHttpServletResponse);

    HttpServletRequest mockHttpServletRequest2 = prepareServletRequest(mockHttpSession2, null, "/test2", Collections.emptyMap());
    ServletRequestContext handler2 = new ServletRequestContext(mockHttpServletRequest2, mockHttpServletResponse, cruiseControlConfigMock);
    try {
      OperationFuture future2 =
              userTaskManager.getOrCreateUserTask(handler2, uuid -> future, 0, true, null).get(0);
      Assert.assertEquals(future, future2);
      EasyMock.verify(mockHttpSession2, mockHttpServletRequest2, mockHttpServletResponse);
    } catch (RuntimeException e) {
      userTaskManager.close();
      return;
    }
    Assert.fail("Don't expect to be here!");
  }

  private HttpServletRequest prepareServletRequest(HttpSession session, String userTaskId) {
    return prepareServletRequest(session, userTaskId, "/test", Collections.emptyMap());
  }

  private HttpServletRequest prepareServletRequest(HttpSession session, String userTaskId, String resource, Map<String, String []> params) {
    HttpServletRequest request = EasyMock.mock(HttpServletRequest.class);
    EasyMock.expect(request.getSession()).andReturn(session).anyTimes();
    EasyMock.expect(request.getSession(false)).andReturn(session).anyTimes();
    EasyMock.expect(request.getMethod()).andReturn(GET_METHOD).anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn(resource).anyTimes();
    EasyMock.expect(request.getPathInfo()).andReturn(resource).anyTimes();
    EasyMock.expect(request.getParameterMap()).andReturn(params).anyTimes();
    EasyMock.expect(request.getHeader(UserTaskManager.USER_TASK_HEADER_NAME)).andReturn(userTaskId).anyTimes();
    EasyMock.expect(request.getRemoteHost()).andReturn("test-host").anyTimes();
    EasyMock.expect(request.getHeaderNames()).andReturn(Collections.enumeration(Collections.emptyList())).anyTimes();
    for (String headerName : KafkaCruiseControlServletUtils.HEADERS_TO_TRY) {
      EasyMock.expect(request.getHeader(headerName)).andReturn("localhost").anyTimes();
    }
    EasyMock.replay(request);
    return request;
  }

  private RoutingContext prepareVertxRequest(Session session, String userTaskId, Http1xServerResponse response) {
    return prepareVertxRequest(session, userTaskId, "/test", Collections.emptyMap(), response);
  }

  private RoutingContext prepareVertxRequest(Session session, String userTaskId, String resource,
                                             Map<String, String []> params, Http1xServerResponse response) {
    MultiMap paramsMultiMap = MultiMap.caseInsensitiveMultiMap();
    for (Map.Entry<String, String[]> entry : params.entrySet()) {
      paramsMultiMap.add((CharSequence) entry.getKey(), Arrays.asList(entry.getValue()));
    }

    RoutingContext context = EasyMock.mock(RoutingContext.class);
    HttpServerRequest mockRequest = EasyMock.mock(HttpServerRequest.class);

    EasyMock.expect(context.request()).andReturn(mockRequest).anyTimes();
    EasyMock.expect(context.session()).andReturn(session).anyTimes();
    EasyMock.expect(context.queryParams()).andReturn(paramsMultiMap).anyTimes();
    EasyMock.expect(context.response()).andReturn(response).anyTimes();

    EasyMock.expect(mockRequest.getHeader(UserTaskManager.USER_TASK_HEADER_NAME)).andReturn(userTaskId).anyTimes();
    EasyMock.expect(mockRequest.method()).andReturn(HttpMethod.GET).anyTimes();
    EasyMock.expect(mockRequest.uri()).andReturn(resource).anyTimes();
    EasyMock.expect(mockRequest.headers()).andReturn(MultiMap.caseInsensitiveMultiMap()).anyTimes();

    for (String headerName : KafkaCruiseControlServletUtils.HEADERS_TO_TRY) {
      EasyMock.expect(mockRequest.getHeader(headerName)).andReturn("localhost").anyTimes();
    }
    EasyMock.replay(context, mockRequest);

    return context;
  }
}
