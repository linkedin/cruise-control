/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;


public class SessionManagerTest {
  
  @Test
  public void testCreateAndCloseSession() {
    TestContext context = prepareRequests(true, 1);
    SessionManager sessionManager = new SessionManager(1, 1000, context.time());

    sessionManager.getAndCreateSessionIfNotExist(context.request(0), 
                                                 () -> new OperationFuture<>("testCreateSession"));
    assertEquals(1, sessionManager.numSessions());

    sessionManager.closeSession(context.request(0));
    assertEquals(0, sessionManager.numSessions());
  }
  
  @Test
  public void testSessionExpiration() {
    TestContext context = prepareRequests(true, 2);
    SessionManager sessionManager = new SessionManager(2, 1000, context.time());
    
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
    SessionManager sessionManager = new SessionManager(1, 1000, context.time());
    
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
