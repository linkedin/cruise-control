/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that helps track the http sessions and their corresponding operations.
 * 
 * When an {@link HttpServletRequest} executes a long operation. The servlet will submit an asynchronous operation and 
 * return the progress of that operation instead of blocking waiting for the operation to finish. In that case, the
 * {@link HttpSession} of that HttpServletRequest will be recorded by the servlet. Next time when the same client 
 * issue the same request again, it will resume the operation requested last time.
 * 
 * If an HttpSession is recorded for a request with a request URL, it is required that the same request URL should be
 * issued again from the same session until the asynchronous operation is finished. Otherwise an exception will
 * be returned.
 */
public class SessionManager {
  private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);
  private final int _capacity;
  private final long _sessionExpiryMs;
  private final Map<HttpSession, SessionInfo> _inProgressSessions;
  private final Time _time;
  private final ScheduledExecutorService _sessionCleaner = 
      Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("SessionCleaner", 
                                                                                     true, 
                                                                                     null));

  /**
   * Construct the session manager.
   * @param capacity the maximum of sessions allowed to exist at the same time.
   * @param sessionExpiryMs the maximum time to wait before expire an inactive session.
   * @param time the time object for unit test.
   * @param dropwizardMetricRegistry the metric registry to record metrics.
   */
  SessionManager(int capacity, long sessionExpiryMs, Time time, MetricRegistry dropwizardMetricRegistry) {
    _capacity = capacity;
    _sessionExpiryMs = sessionExpiryMs;
    _time = time;
    _inProgressSessions = new HashMap<>();
    _sessionCleaner.scheduleAtFixedRate(new ExpiredSessionCleaner(), 0, 5, TimeUnit.SECONDS);
    dropwizardMetricRegistry.register(MetricRegistry.name("SessionManager", "num-active-sessions"),
                                      (Gauge<Integer>) _inProgressSessions::size);
    
  }

  /**
   * Close the session manager.
   */
  public void close() {
    _sessionCleaner.shutdownNow();
  }

  /**
   * @return Total number of active sessions.
   */
  synchronized int numSessions() {
    return _inProgressSessions.size();
  }

  /**
   * Create the session for the request if needed.
   * 
   * @param request the HttpServletRequest to create session for.
   * @param operation the async operation that returns an {@link OperationFuture}
   *                  
   * @return the {@link OperationFuture} for the provided async operation.
   */
  @SuppressWarnings("unchecked")
  synchronized <T> OperationFuture<T> getAndCreateSessionIfNotExist(HttpServletRequest request,
                                                                    Supplier<OperationFuture<T>> operation) {
    HttpSession session = request.getSession();
    SessionInfo info = _inProgressSessions.get(session);
    if (info != null) {
      LOG.debug("Found existing session {}", session);
      if (!info.requestUrl().equals(request.toString())) {
        throw new UserRequestException("The session has an ongoing operation " + info.requestUrl() 
                                           + " while it " + "is trying another operation of " + request.toString());
      } else {
        return (OperationFuture<T>) info.future();
      }
    } else {
      if (_inProgressSessions.size() >= _capacity) {
        throw new RuntimeException("There are already " + _inProgressSessions.size() + " active sessions, which "
                                       + "has reached the servlet capacity.");
      }
      LOG.debug("Created session for {}", session);
      info = new SessionInfo(null, request.toString());
      OperationFuture<T> future = operation.get();
      info.setFuture(future);
      _inProgressSessions.put(session, info);
      return future;
    }
  }

  /**
   * Get the {@link OperationFuture} for the request.
   * @param request the request to get the operation future.
   * @param <T> the returned future type.
   * @return the operation future for the request if it exists, otherwise <tt>null</tt> is returned.
   */
  @SuppressWarnings("unchecked")
  synchronized <T> T getFuture(HttpServletRequest request) {
    SessionInfo info = _inProgressSessions.get(request.getSession());
    if (info == null) {
      return null;
    } else if (!info.requestUrl().equals(request.toString())) {
      throw new UserRequestException("The session has an ongoing operation " + info.requestUrl() + " while it "
                                         + "is trying another operation of " + request.toString());
    }
    return (T) info.future();
  }

  /**
   * Close the session for the given request.
   * @param request the request to close its session.
   */
  synchronized void closeSession(HttpServletRequest request) {
    HttpSession session = request.getSession();
    if (session == null) {
      return;
    }
    SessionInfo info = _inProgressSessions.remove(session);
    if (info != null && info.future().isDone()) {
      LOG.debug("Closing session {}", session);
      session.invalidate();
    }
  }

  /**
   * Expire the sessions that have been inactive for more than configured expiration time.
   */
  synchronized void expireOldSessions() {
    long now = _time.milliseconds();
    Iterator<Map.Entry<HttpSession, SessionInfo>> iter = _inProgressSessions.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<HttpSession, SessionInfo> entry = iter.next();
      HttpSession session = entry.getKey();
      SessionInfo info = entry.getValue();
      LOG.trace("Session {} was last accessed at {}, age is {} ms", session, session.getLastAccessedTime(), 
                now - session.getLastAccessedTime());
      if (now >= session.getLastAccessedTime() + _sessionExpiryMs) {
        LOG.debug("Expiring session {}.", session);
        iter.remove();
        session.invalidate();
        info.future().cancel(true);
      }
    }
  }

  /**
   * Container class to host the future and the request url for verification.
   */
  private static class SessionInfo {
    private final String _requestUrl;
    private OperationFuture _operationFuture;
    
    private SessionInfo(OperationFuture operationFuture, String requestUrl) {
      _operationFuture = operationFuture;
      _requestUrl = requestUrl;
    }
    
    private void setFuture(OperationFuture future) {
      _operationFuture = future;
    }
    
    private OperationFuture future() {
      return _operationFuture;
    }
    
    private String requestUrl() {
      return _requestUrl;
    }
    
  }

  /**
   * A runnable class to expire the old sessions.
   */
  private class ExpiredSessionCleaner implements Runnable {
    @Override
    public void run() {
      try {
        expireOldSessions();
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to expire sessions.", t);
      }
    }
  }
}
