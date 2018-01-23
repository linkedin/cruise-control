/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class SessionManagerTest {

  @Test
  public void testCreateAndCloseSession() {
    TestContext context = prepareRequests(true, 1);
    SessionManager sessionManager = new SessionManager(1, 1000, context.time(), new MetricRegistry());

    sessionManager.getAndCreateSessionIfNotExist(context.request(0),
                                                 () -> new OperationFuture<>("testCreateSession"));
    assertEquals(1, sessionManager.numSessions());

    sessionManager.closeSession(context.request(0));
    assertEquals(0, sessionManager.numSessions());
  }

  @Test
  public void testSessionExpiration() {
    TestContext context = prepareRequests(true, 2);
    SessionManager sessionManager = new SessionManager(2, 1000, context.time(), new MetricRegistry());

    List<OperationFuture<Integer>> futures = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      futures.add(sessionManager.getAndCreateSessionIfNotExist(context.request(i),
                                                               () -> new OperationFuture<>("testSessionExpiration")));
    }
    assertEquals(2, sessionManager.numSessions());

    // Sleep to 1 ms before expiration.
    context.time().sleep(999);
    sessionManager.expireOldSessions();
    assertEquals(2, sessionManager.numSessions());
    for (Future future : futures)  {
      assertFalse(future.isDone());
      assertFalse(future.isCancelled());
    }
    // Sleep to the exact time to expire
    context.time().sleep(1);
    sessionManager.expireOldSessions();
    assertEquals("All the sessions should have been expired", 0, sessionManager.numSessions());
    for (Future future : futures)  {
      assertTrue(future.isDone());
      assertTrue(future.isCancelled());
    }
  }

  @Test
  public void testCreateSessionReachingCapacity() {
    TestContext context = prepareRequests(false, 2);
    SessionManager sessionManager = new SessionManager(1, 1000, context.time(), new MetricRegistry());

    sessionManager.getAndCreateSessionIfNotExist(context.request(0),
                                                 () -> new OperationFuture<>("testCreateSession"));
    assertEquals(1, sessionManager.numSessions());
    assertNull(sessionManager.getFuture(context.request(1)));
    // Adding same request again should have no impact
    sessionManager.getAndCreateSessionIfNotExist(context.request(0),
                                                 () -> new OperationFuture<>("testCreateSession"));
    assertEquals(1, sessionManager.numSessions());

    try {
      sessionManager.getAndCreateSessionIfNotExist(context.request(1),
                                                   () -> new OperationFuture<>("testCreateSession"));
      fail("Should have thrown exception due to session capacity reached.");
    } catch (RuntimeException e) {
      // let it go.
    }
  }

  @Test
  public void testMultiThreadOnSameSession() throws InterruptedException {
    TestContext context = prepareRequests(false, 1);
    SessionManager sessionManager = new SessionManager(1, 1000, context.time(), new MetricRegistry());

    // Lock the session.
    sessionManager.getAndCreateSessionIfNotExist(context.request(0),
                                                 () -> new OperationFuture<>("testMultiThreadOnSameSession"));

    // The other thread should fail to lock.
    AtomicBoolean receivedException = new AtomicBoolean(false);
    tryReinitSessionAndLock(sessionManager, context.request(0), receivedException);
    assertTrue(receivedException.get());

    // After unlock the session, the other thread will be able to reinit the session and lock it.
    sessionManager.unLockSession(context.request(0));
    tryReinitSessionAndLock(sessionManager, context.request(0), receivedException);
    assertFalse(receivedException.get());

    // Should throw exception because the session is locked by another thread.
    try {
      sessionManager.reinitAndLockSession(context.request(0));
      fail("Should have thrown exception.");
    } catch (IllegalStateException ise) {
      // let it go.
    }
  }

  @Test
  public void testMultipleOperationRequest() {
    TestContext context = prepareRequests(false, 1);
    SessionManager sessionManager = new SessionManager(1, 1000, context.time(), new MetricRegistry());
    HttpServletRequest request = context.request(0);
    OperationFuture<Integer> future1 = new OperationFuture<>("future1");
    OperationFuture<String> future2 = new OperationFuture<>("future2");
    // Create the first future.
    OperationFuture<Integer> intFuture = sessionManager.getAndCreateSessionIfNotExist(request, () -> future1);
    assertTrue(intFuture == future1);
    future1.complete(100);
    // create the second future.
    OperationFuture<String> stringFuture = sessionManager.getAndCreateSessionIfNotExist(request, () -> future2);
    assertTrue(stringFuture == future2);
    future2.complete("abc");
    sessionManager.unLockSession(request);

    // Now reinit the session and see if we can get the results in order.
    sessionManager.reinitAndLockSession(request);
    intFuture = sessionManager.getAndCreateSessionIfNotExist(request, () -> new OperationFuture<>("future3"));
    assertTrue(intFuture == future1);
    stringFuture = sessionManager.getAndCreateSessionIfNotExist(request, () -> new OperationFuture<>("future4"));
    assertTrue(stringFuture == future2);

  }

  private void tryReinitSessionAndLock(SessionManager sessionManager,
                                      HttpServletRequest request,
                                      AtomicBoolean receivedException) throws InterruptedException {
    receivedException.set(false);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          sessionManager.reinitAndLockSession(request);
        } catch (IllegalStateException ise) {
          receivedException.set(true);
        }
      }
    };
    t.start();
    t.join();
  }

  private TestContext prepareRequests(boolean expectSessionInvalidation, int numRequests) {
    Time time = new MockTime();
    List<HttpServletRequest> requests = new ArrayList<>(numRequests);
    List<HttpSession> sessions = new ArrayList<>();
    for (int i = 0; i < numRequests; i++) {
      HttpServletRequest request = EasyMock.mock(HttpServletRequest.class);
      HttpSession session = EasyMock.mock(HttpSession.class);
      requests.add(request);
      sessions.add(session);
      EasyMock.expect(request.getSession()).andReturn(session).anyTimes();
      EasyMock.expect(request.getMethod()).andReturn("GET").anyTimes();
      EasyMock.expect(request.getRequestURI()).andReturn("/test").anyTimes();
      EasyMock.expect(session.getLastAccessedTime()).andReturn(time.milliseconds()).anyTimes();
      if (expectSessionInvalidation) {
        session.invalidate();
        EasyMock.expectLastCall().once();
      }
    }
    EasyMock.replay(requests.toArray());
    EasyMock.replay(sessions.toArray());
    return new TestContext(requests, time);
  }

  private static class TestContext {
    private List<HttpServletRequest> _requests;
    private Time _time;

    private TestContext(List<HttpServletRequest> requests, Time time) {
      _requests = requests;
      _time = time;
    }

    private HttpServletRequest request(int index) {
      return _requests.get(index);
    }

    private Time time() {
      return _time;
    }
  }
}
