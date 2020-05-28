/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final Timer _sessionLifetimeTimer;
  private final Meter _sessionCreationFailureMeter;
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer;

  /**
   * Construct the session manager.
   * @param capacity the maximum of sessions allowed to exist at the same time.
   * @param sessionExpiryMs the maximum time to wait before expire an inactive session.
   * @param time the time object for unit test.
   * @param dropwizardMetricRegistry the metric registry to record metrics.
   */
  SessionManager(int capacity,
                 long sessionExpiryMs,
                 Time time,
                 MetricRegistry dropwizardMetricRegistry,
                 Map<EndPoint, Timer> successfulRequestExecutionTimer) {
    _capacity = capacity;
    _sessionExpiryMs = sessionExpiryMs;
    _time = time;
    _inProgressSessions = new HashMap<>();
    _sessionCleaner.scheduleAtFixedRate(new ExpiredSessionCleaner(), 0, 5, TimeUnit.SECONDS);
    _successfulRequestExecutionTimer = successfulRequestExecutionTimer;
    // Metrics registration
    _sessionLifetimeTimer = dropwizardMetricRegistry.timer(MetricRegistry.name("SessionManager", "session-lifetime-timer"));
    _sessionCreationFailureMeter = dropwizardMetricRegistry.meter(MetricRegistry.name("SessionManager", "session-creation-failure-rate"));
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
   * @param step the index of the step whose future needs to be added or get.
   *
   * @return The {@link OperationFuture} for the provided async operation.
   */
  @SuppressWarnings("unchecked")
  synchronized OperationFuture getAndCreateSessionIfNotExist(HttpServletRequest request,
                                                             Supplier<OperationFuture> operation,
                                                             int step) {
    HttpSession session = request.getSession();
    SessionInfo info = _inProgressSessions.get(session);
    String requestString = toRequestString(request);
    // Session exists.
    if (info != null) {
      LOG.info("Found existing session {}", session);
      info.ensureSameRequest(requestString, request.getParameterMap());
      // If there is next future return it.
      if (step < info.numFutures()) {
        return info.future(step);
      } else if (step == info.numFutures()) {
        LOG.info("Adding new future to existing session {}.", session);
        // if there is no next future, add the future to the next list.
        OperationFuture future = operation.get();
        info.addFuture(future);
        return future;
      } else {
        throw new IllegalArgumentException(String.format("There are %d steps in the session. Cannot add step %d.",
                                                         info.numFutures(), step));
      }
    } else {
      if (step > 0) {
        throw new IllegalArgumentException(String.format("There are no step in the session. Cannot add step %d.", step));
      }
      // The session does not exist, add it.
      if (_inProgressSessions.size() >= _capacity) {
        _sessionCreationFailureMeter.mark();
        throw new RuntimeException("There are already " + _inProgressSessions.size() + " active sessions, which "
                                   + "has reached the servlet capacity.");
      }
      LOG.info("Created session for {}", session);
      info = new SessionInfo(requestString, request.getParameterMap(), ParameterUtils.endPoint(request));
      OperationFuture future = operation.get();
      info.addFuture(future);
      _inProgressSessions.put(session, info);
      return future;
    }
  }

  /**
   * Get the {@link OperationFuture} for the request.
   * @param request the request to get the operation future.
   * @param <T> the returned future type.
   * @return The operation future for the request if it exists, otherwise {@code null} is returned.
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
   * Close the session for the given request.
   * @param request the request to close its session.
   * @param hasError whether the session is closed due to an error or not.
   */
  synchronized void closeSession(HttpServletRequest request, boolean hasError) {
    // Response associated with this request has already been flushed; hence, do not attempt to create a new session.
    HttpSession session = request.getSession(false);
    if (session == null) {
      return;
    }
    SessionInfo info = _inProgressSessions.remove(session);
    if (info != null && info.lastFuture().isDone()) {
      LOG.info("Closing session {}", session);
      session.invalidate();
      _sessionLifetimeTimer.update(System.nanoTime() - info.requestStartTimeNs(), TimeUnit.NANOSECONDS);
      if (!hasError && info.executionTime() > 0) {
        _successfulRequestExecutionTimer.get(info.endPoint()).update(info.executionTime(), TimeUnit.NANOSECONDS);
      }
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
      if (LOG.isTraceEnabled()) {
        LOG.trace("Session {} was last accessed at {}, age is {} ms", session, session.getLastAccessedTime(),
                  now - session.getLastAccessedTime());
      }
      if (now >= session.getLastAccessedTime() + _sessionExpiryMs) {
        LOG.info("Expiring session {}.", session);
        iter.remove();
        session.invalidate();
        _sessionLifetimeTimer.update(System.nanoTime() - info.requestStartTimeNs(), TimeUnit.NANOSECONDS);
        if (info.lastFuture().isDone() && info.executionTime() > 0) {
          _successfulRequestExecutionTimer.get(info.endPoint()).update(info.executionTime(), TimeUnit.NANOSECONDS);
        } else {
          info.lastFuture().cancel(true);
        }
      }
    }
  }

  /**
   * Container class to host the future and the request url for verification.
   */
  private static class SessionInfo {
    private final String _requestUrl;
    private final Map<String, String[]> _requestParameters;
    private final List<OperationFuture> _operationFuture;
    private final long _requestStartTimeNs;
    private final EndPoint _endPoint;

    private SessionInfo(String requestUrl, Map<String, String[]> requestParameters, EndPoint endPoint) {
      _operationFuture = new ArrayList<>();
      _requestUrl = requestUrl;
      _requestParameters = requestParameters;
      _requestStartTimeNs = System.nanoTime();
      _endPoint = endPoint;
    }

    private int numFutures() {
      return _operationFuture.size();
    }

    private void addFuture(OperationFuture future) {
      _operationFuture.add(future);
    }

    private OperationFuture future(int index) {
      return _operationFuture.get(index);
    }

    private OperationFuture lastFuture() {
      return _operationFuture.get(_operationFuture.size() - 1);
    }

    private String requestUrl() {
      return _requestUrl;
    }

    private long requestStartTimeNs() {
      return _requestStartTimeNs;
    }

    private long executionTime() {
      return lastFuture().finishTimeNs() == -1 ? -1 : lastFuture().finishTimeNs() - _requestStartTimeNs;
    }

    private EndPoint endPoint() {
      return _endPoint;
    }

    private boolean paramEquals(Map<String, String[]> parameters) {
      boolean isSameParameters = _requestParameters.keySet().equals(parameters.keySet());
      if (isSameParameters) {
        for (Map.Entry<String, String[]> entry : _requestParameters.entrySet()) {
          Set<String> param1 = new HashSet<>(Arrays.asList(entry.getValue()));
          Set<String> param2 = new HashSet<>(Arrays.asList(parameters.get(entry.getKey())));
          if (!param1.equals(param2)) {
            return false;
          }
        }
      }
      return isSameParameters;
    }

    private void ensureSameRequest(String requestUrl, Map<String, String[]> parameters) {
      if (!_requestUrl.equals(requestUrl) || !paramEquals(parameters)) {
        throw new IllegalStateException(String.format(
            "The session has an ongoing operation [URL: %s, Parameters: %s] "
            + "while it is trying another operation of [URL: %s, Parameters: %s].",
            _requestUrl, _requestParameters, requestUrl, parameters));
      }
    }
  }

  private static String toRequestString(HttpServletRequest request) {
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
