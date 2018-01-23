/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
    String requestString = toRequestString(request);
    // Session exists.
    if (info != null) {
      LOG.debug("Found existing session {}", session);
      if (!info.requestUrl().equals(requestString)) {
        throw new IllegalStateException("The session has an ongoing operation " + info.requestUrl() +
                                            " while it is trying another operation of " + requestString);
      }
      // If there is next future return it.
      if (info.hasNextFuture()) {
        return (OperationFuture<T>) info.nextFuture();
      } else {
        LOG.debug("Adding new future to existing session {}.", session);
        // if there is no next future, add the future to the next list.
        OperationFuture<T> future = operation.get();
        info.addFuture(future);
        return future;
      }
    } else {
      // The session does not exist, add it.
      if (_inProgressSessions.size() >= _capacity) {
        throw new RuntimeException("There are already " + _inProgressSessions.size() + " active sessions, which "
                                       + "has reached the servlet capacity.");
      }
      LOG.debug("Created session for {}", session);
      info = new SessionInfo(requestString);
      OperationFuture<T> future = operation.get();
      info.addFuture(future);
      _inProgressSessions.put(session, info);
      return (OperationFuture<T>) info.nextFuture();
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
    } else if (!info.requestUrl().equals(toRequestString(request))) {
      throw new IllegalStateException("The session has an ongoing operation " + info.requestUrl() + " while it "
                                         + "is trying another operation of " + toRequestString(request));
    }
    return (T) info.lastFuture();
  }

  /**
   * Reinitialize the session state and lock the session to avoid the same session being used by another thread.
   *
   * @param request the request whose session needs to be reinitialized and locked.
   */
  synchronized void reinitAndLockSession(HttpServletRequest request) {
    SessionInfo info = _inProgressSessions.get(request.getSession());
    if (info != null) {
      info.lockSession();
      info.resetIndex();
    }
  }

  /**
   * Unlock the session of the request.
   * @param request the request whose session needs to be unlocked.
   */
  synchronized void unLockSession(HttpServletRequest request) {
    SessionInfo info = _inProgressSessions.get(request.getSession());
    if (info != null) {
      info.unlockSession();
    }
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
    if (info != null && info.lastFuture().isDone()) {
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
        info.lastFuture().cancel(true);
      }
    }
  }

  /**
   * Container class to host the future and the request url for verification.
   */
  private static class SessionInfo {
    private final String _requestUrl;
    private final List<OperationFuture> _operationFuture;
    private Thread _lockedBy;
    private int _index;

    private SessionInfo(String requestUrl) {
      _index = 0;
      _lockedBy = Thread.currentThread();
      _operationFuture = new ArrayList<>();
      _requestUrl = requestUrl;
    }

    private void lockSession() {
      if (_lockedBy != null && _lockedBy != Thread.currentThread()) {
        throw new IllegalStateException("The session is locked by another thread.");
      }
      _lockedBy = Thread.currentThread();
    }

    private void unlockSession() {
      if (_lockedBy != null && _lockedBy == Thread.currentThread()) {
        _lockedBy = null;
      }
    }

    private void addFuture(OperationFuture future) {
      _operationFuture.add(future);
    }

    private boolean hasNextFuture() {
      return _index < _operationFuture.size();
    }

    private void resetIndex() {
      _index = 0;
    }

    private OperationFuture nextFuture() {
      OperationFuture future = _operationFuture.get(_index);
      _index++;
      return future;
    }

    private OperationFuture lastFuture() {
      return _operationFuture.get(_operationFuture.size() - 1);
    }

    private String requestUrl() {
      return _requestUrl;
    }

  }

  private String toRequestString(HttpServletRequest request) {
    return String.format("%s(%s %s)",
                         request.getClass().getSimpleName(),
                         request.getMethod(),
                         request.getRequestURI());
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
