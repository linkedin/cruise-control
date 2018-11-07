/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link UserTaskManager} keeps track of long-running request.
 *
 * {@link HttpServletRequest} can execute for long durations. The servlet submits the asynchronous tasks and returns the
 * progress of the operation instead of blocking for the operation to complete. {@link UserTaskManager} maintains the
 * mapping of Request URL and {@link HttpSession} to UserTaskID({@link UUID}). To fetch the status of a request, the
 * client can use the same Request ULR along with session cookie to retrieve the current status. The status of the
 * request can also be fetched using the UserTaskID. '/user_tasks' endpoint can be used to fetch all the active and
 * recently completed UserTasks.
 */
public class UserTaskManager implements Closeable {
  public static final String USER_TASK_HEADER_NAME = "User-Task-ID";
  public static final long USER_TASK_SCANNER_PERIOD_SECONDS = 5;
  public static final long USER_TASK_SCANNER_INITIAL_DELAY_SECONDS = 0;

  private static final Logger LOG = LoggerFactory.getLogger(UserTaskManager.class);
  private final Map<SessionKey, UUID> _sessionToUserTaskIdMap;
  private final Map<UUID, UserTaskInfo> _activeUserTaskIdToFuturesMap;
  private final Map<UUID, UserTaskInfo> _completedUserTaskIdToFuturesMap;
  private final long _sessionExpiryMs;
  private final long _maxActiveUserTasks; // maximum number of active async user operations across all users
  private final Time _time;
  private final ScheduledExecutorService _userTaskScannerExecutor =
      Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("UserTaskScanner", true, null));
  private final UUIDGenerator _uuidGenerator;
  private static final Comparator<UUID> USER_TASK_ID_COMPARATOR = new Comparator<UUID>() {
    @Override
    public int compare(UUID o1, UUID o2) {
      return o1.compareTo(o2);
    }
  };
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer;
  private final long _completedUserTaskRetentionTimeMs;

  public UserTaskManager(long sessionExpiryMs,
                         long maxActiveUserTasks,
                         long completedUserTaskRetentionTimeMs,
                         MetricRegistry dropwizardMetricRegistry,
                         Map<EndPoint, Timer> successfulRequestExecutionTimer) {
    _sessionToUserTaskIdMap = new HashMap<>();
    _activeUserTaskIdToFuturesMap = new TreeMap<>(USER_TASK_ID_COMPARATOR);
    _completedUserTaskIdToFuturesMap = new TreeMap<>(USER_TASK_ID_COMPARATOR);
    _sessionExpiryMs = sessionExpiryMs;
    _maxActiveUserTasks = maxActiveUserTasks;
    _completedUserTaskRetentionTimeMs = completedUserTaskRetentionTimeMs;
    _time = Time.SYSTEM;
    _uuidGenerator = new UUIDGenerator();
    _userTaskScannerExecutor.scheduleAtFixedRate(new UserTaskScanner(),
                                                 USER_TASK_SCANNER_INITIAL_DELAY_SECONDS,
                                                 USER_TASK_SCANNER_PERIOD_SECONDS,
                                                 TimeUnit.SECONDS);
    dropwizardMetricRegistry.register(MetricRegistry.name("UserTaskManager", "num-active-sessions"),
                                      (Gauge<Integer>) _sessionToUserTaskIdMap::size);
    dropwizardMetricRegistry.register(MetricRegistry.name("UserTaskManager", "num-active-user-tasks"),
                                      (Gauge<Integer>) _activeUserTaskIdToFuturesMap::size);
    _successfulRequestExecutionTimer = successfulRequestExecutionTimer;
  }

  // for unit-tests only
  UserTaskManager(long sessionExpiryMs,
                  long maxActiveUserTasks,
                  long completedUserTaskRetentionTimeMs,
                  Time time,
                  UUIDGenerator uuidGenerator) {
    _sessionToUserTaskIdMap = new HashMap<>();
    _activeUserTaskIdToFuturesMap = new HashMap<>();
    _completedUserTaskIdToFuturesMap = new HashMap<>();
    _sessionExpiryMs = sessionExpiryMs;
    _maxActiveUserTasks = maxActiveUserTasks;
    _completedUserTaskRetentionTimeMs = completedUserTaskRetentionTimeMs;
    _time = time;
    _uuidGenerator = uuidGenerator;
    _userTaskScannerExecutor.scheduleAtFixedRate(new UserTaskScanner(),
                                                 USER_TASK_SCANNER_INITIAL_DELAY_SECONDS,
                                                 USER_TASK_SCANNER_PERIOD_SECONDS,
                                                 TimeUnit.SECONDS);
    _successfulRequestExecutionTimer = new HashMap<>();
    EndPoint.cachedValues().stream().forEach(e -> _successfulRequestExecutionTimer.put(e, new Timer()));
  }

  // for unit-tests only
  UserTaskManager(long sessionExpiryMs,
                  long maxActiveUserTasks,
                  long completedUserTaskRetentionTimeMs,
                  Time time) {
    this(sessionExpiryMs, maxActiveUserTasks, completedUserTaskRetentionTimeMs, time, new UUIDGenerator());
  }

  private static String httpServletRequestToString(HttpServletRequest request) {
    return String.format("%s %s", request.getMethod(), request.getRequestURI());
  }

  /**
   * Create the UserTaskInfo reference if it doesn't exist.
   *
   * This method creates references {@link UserTaskInfo} and maps it to {@link HttpSession} from httpServletRequest. The
   * {@link HttpSession} is also used to fetch {@link OperationFuture} of UserTask that is already in progress. If the
   * UserTaskID is passed in the httpServletRequest header then that takes precedence over {@link HttpSession} to fetch
   * the {@link OperationFuture} for the UserTask.
   *
   * @param httpServletRequest the HttpServletRequest to create the UserTaskInfo reference.
   * @param httpServletResponse the HttpServletResponse that contains the UserTaskId in the HttpServletResponse header.
   * @param operation An asynchronous operations that returns {@link OperationFuture}.
   * @param step The index of the step that has to be added or fetched.
   * @return The {@link OperationFuture} for the provided asynchronous operation.
   */
  @SuppressWarnings("unchecked")
  public <T> OperationFuture<T> getOrCreateUserTask(HttpServletRequest httpServletRequest,
                                                    HttpServletResponse httpServletResponse,
                                                    Supplier<OperationFuture<T>> operation,
                                                    int step) {
    UUID userTaskId = getUserTaskId(httpServletRequest);
    List<OperationFuture> operationFutures = getFuturesByUserTaskId(userTaskId, httpServletRequest);

    if (operationFutures != null) {
      LOG.info("Fetch an existing UserTask {}", userTaskId);
      httpServletResponse.setHeader(USER_TASK_HEADER_NAME, userTaskId.toString());
      if (step < operationFutures.size()) {
        return (OperationFuture<T>) operationFutures.get(step);
      } else if (step == operationFutures.size()) {
        LOG.info("Add a new future to existing UserTask {}", userTaskId);
        OperationFuture future = operation.get();
        insertFuturesByUserTaskId(userTaskId, future, httpServletRequest);
        return future;
      } else {
        throw new IllegalArgumentException(
            String.format("There are %d steps in the session. Cannot add step %d.", operationFutures.size(), step));
      }
    } else {
      if (httpServletRequest.getHeader(USER_TASK_HEADER_NAME) != null) {
        // request provides user_task_header that user tasks doesn't exist then just throw exception
        String userTaskIdFromRequest = httpServletRequest.getHeader(USER_TASK_HEADER_NAME);
        throw new IllegalArgumentException(
            String.format("UserTask %s is an invalid %s", userTaskIdFromRequest, USER_TASK_HEADER_NAME));
      }

      if (step != 0) {
        throw new IllegalArgumentException(
            String.format("There are no step in the session. Cannot add step %d.", step));
      }

      SessionKey sessionKey = new SessionKey(httpServletRequest);
      userTaskId = _uuidGenerator.randomUUID();
      LOG.info("Create a new UserTask {} with SessionKey {}", userTaskId, sessionKey);

      OperationFuture future = operation.get();
      insertFuturesByUserTaskId(userTaskId, future, httpServletRequest);

      synchronized (_sessionToUserTaskIdMap) {
        _sessionToUserTaskIdMap.put(sessionKey, userTaskId);
      }

      httpServletResponse.setHeader(USER_TASK_HEADER_NAME, userTaskId.toString());
      return future;
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T getFuture(HttpServletRequest request) {
    UUID userTaskId = getUserTaskId(request);
    List<OperationFuture> operationFutures = getFuturesByUserTaskId(userTaskId, request);
    if (operationFutures == null || operationFutures.isEmpty()) {
      return null;
    }

    return (T) operationFutures.get(operationFutures.size() - 1);
  }

  private void expireOldSessions() {
    long now = _time.milliseconds();
    synchronized (_sessionToUserTaskIdMap) {
      Iterator<Map.Entry<SessionKey, UUID>> iter = _sessionToUserTaskIdMap.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<SessionKey, UUID> entry = iter.next();
        SessionKey sessionKey = entry.getKey();
        HttpSession session = sessionKey.httpSession();
        LOG.trace("Session {} was last accessed at {}, age is {} ms", session, session.getLastAccessedTime(),
            now - session.getLastAccessedTime());
        if (now >= session.getLastAccessedTime() + _sessionExpiryMs) {
          LOG.info("Expiring SessionKey {}", entry.getKey());
          iter.remove();
          session.invalidate();
        }
      }
    }
  }

  /**
   * Method returns the user task id based on the {@link HttpServletRequest}. This method tries to find
   * the User-Task-ID from the request header and check if there is any UserTask with the same User-Task-ID.
   * If no User-Task-ID is passed then the {@link HttpSession} is used to fetch the User-Task-ID.
   *
   * @param httpServletRequest the HttpServletRequest to fetch the User-Task-ID and HTTPSession.
   * @return UUID of the user tasks or null if user task doesn't exist.
   */
  public UUID getUserTaskId(HttpServletRequest httpServletRequest) {
    String userTaskIdString = httpServletRequest.getHeader(USER_TASK_HEADER_NAME);

    UUID userTaskId;
    if (userTaskIdString != null && !userTaskIdString.isEmpty()) { // valid user task id
      userTaskId = UUID.fromString(userTaskIdString);
    } else {
      SessionKey sessionKey = new SessionKey(httpServletRequest);
      synchronized (_sessionToUserTaskIdMap) {
        userTaskId = _sessionToUserTaskIdMap.get(sessionKey);
      }
    }

    return userTaskId;
  }

  private synchronized boolean isActiveUserTasksDone(UUID userTaskId) {
    UserTaskInfo userTaskInfo = _activeUserTaskIdToFuturesMap.get(userTaskId);
    if (userTaskInfo == null || userTaskInfo.futures().isEmpty()) {
      return true;
    }

    List<OperationFuture> futures = userTaskInfo.futures();
    return futures.get(futures.size() - 1).isDone();
  }

  private synchronized void checkActiveUserTasks() {
    Iterator<Map.Entry<UUID, UserTaskInfo>> iter = _activeUserTaskIdToFuturesMap.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<UUID, UserTaskInfo> entry = iter.next();
      if (isActiveUserTasksDone(entry.getKey())) {
        LOG.info("UserTask {} is complete and removed from active tasks list", entry.getKey());
        _successfulRequestExecutionTimer.get(entry.getValue().endPoint()).update(entry.getValue().executionTimeNs(), TimeUnit.NANOSECONDS);
        _completedUserTaskIdToFuturesMap.put(entry.getKey(), entry.getValue());
        iter.remove();
      }
    }
  }

  private synchronized void removeOldUserTasks() {
    LOG.debug("Remove old user tasks");
    _completedUserTaskIdToFuturesMap.entrySet().removeIf(entry -> (entry.getValue().startMs()
                                                                   + _completedUserTaskRetentionTimeMs < _time.milliseconds()));
  }

  synchronized List<OperationFuture> getFuturesByUserTaskId(UUID userTaskId, HttpServletRequest httpServletRequest) {
    if (userTaskId == null) {
      return null;
    }

    String requestUrl = httpServletRequestToString(httpServletRequest);
    if (_completedUserTaskIdToFuturesMap.containsKey(userTaskId)) {
      UserTaskInfo userTaskInfo = _completedUserTaskIdToFuturesMap.get(userTaskId);
      if (userTaskInfo.requestUrl().equals(requestUrl)
          && hasTheSameHttpParameter(userTaskInfo.queryParams(), httpServletRequest.getParameterMap())) {
        return userTaskInfo.futures();
      }
    }

    if (_activeUserTaskIdToFuturesMap.containsKey(userTaskId)) {
      UserTaskInfo userTaskInfo = _activeUserTaskIdToFuturesMap.get(userTaskId);
      if (userTaskInfo.requestUrl().equals(requestUrl)
          && hasTheSameHttpParameter(userTaskInfo.queryParams(), httpServletRequest.getParameterMap())) {
        return userTaskInfo.futures();
      }
    }

    return null;
  }

  private synchronized void insertFuturesByUserTaskId(UUID userTaskId,
                                                      OperationFuture operationFuture,
                                                      HttpServletRequest httpServletRequest) {
    if (_activeUserTaskIdToFuturesMap.containsKey(userTaskId)) {
      _completedUserTaskIdToFuturesMap.remove(userTaskId);
      _activeUserTaskIdToFuturesMap.get(userTaskId).futures().add(operationFuture);
    } else {
      if (_activeUserTaskIdToFuturesMap.size() >= _maxActiveUserTasks) {
        throw new RuntimeException("There are already " + _activeUserTaskIdToFuturesMap.size() + " active user tasks, which has reached the servlet capacity.");
      }
      UserTaskInfo userTaskInfo =
          new UserTaskInfo(httpServletRequest, new ArrayList<>(Collections.singleton(operationFuture)),
              _time.milliseconds(), userTaskId);
      _activeUserTaskIdToFuturesMap.put(userTaskId, userTaskInfo);
    }
  }

  public synchronized List<UserTaskInfo> getActiveUserTasks() {
    return new ArrayList<>(_activeUserTaskIdToFuturesMap.values());
  }

  public synchronized List<UserTaskInfo> getCompletedUserTasks() {
    return new ArrayList<>(_completedUserTaskIdToFuturesMap.values());
  }

  @Override
  public String toString() {
    return "UserTaskManager{_sessionToUserTaskIdMap=" + _sessionToUserTaskIdMap
        + ", _activeUserTaskIdToFuturesMap=" + _activeUserTaskIdToFuturesMap + ", _completedUserTaskIdToFuturesMap="
        + _completedUserTaskIdToFuturesMap + '}';
  }

  @Override
  public void close() {
    _userTaskScannerExecutor.shutdownNow();
  }

  private boolean hasTheSameHttpParameter(Map<String, String[]> params1, Map<String, String[]> params2) {
    boolean isSameParameters = params1.keySet().equals(params2.keySet());
    if (isSameParameters) {
      for (Map.Entry<String, String[]> entry : params1.entrySet()) {
        Set<String> values1 = new HashSet<>(Arrays.asList(entry.getValue()));
        Set<String> values2 = new HashSet<>(Arrays.asList(params2.get(entry.getKey())));
        if (!values1.equals(values2)) {
          return false;
        }
      }
    }
    return isSameParameters;
  }

  // for unit-test only
  int numActiveSessions() {
    return _sessionToUserTaskIdMap.size();
  }

  static public class SessionKey {
    private final HttpSession _httpSession;
    private final String _requestUrl;
    private final Map<String, Set<String>> _queryParams;

    SessionKey(HttpServletRequest httpServletRequest) {
      _httpSession = httpServletRequest.getSession();
      _requestUrl = httpServletRequestToString(httpServletRequest);
      _queryParams = new HashMap<>();
      httpServletRequest.getParameterMap().forEach((k, v) -> _queryParams.put(k, new HashSet<>(Arrays.asList(v))));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SessionKey that = (SessionKey) o;
      return Objects.equals(_httpSession, that._httpSession)
             && Objects.equals(_requestUrl, that._requestUrl)
             && Objects.equals(_queryParams, that._queryParams);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_httpSession, _requestUrl, _queryParams);
    }

    @Override
    public String toString() {
      return String.format("SessionKey{_httpSession=%s,_requestUrl=%s,_queryParams=%s}", _httpSession, _requestUrl,
                           _queryParams);
    }

    public HttpSession httpSession() {
      return _httpSession;
    }
  }

  /**
   * A internal class for generating random UUID for user tasks.
   */
  public static class UUIDGenerator {
    UUID randomUUID() {
      return UUID.randomUUID();
    }
  }

  /**
   * A class to encapsulate UserTask.
   */
  public static class UserTaskInfo {
    private final List<OperationFuture> _futures;
    private final String _requestUrl;
    private final String _clientIdentity;
    private final long _startMs;
    private final UUID _userTaskId;
    private final Map<String, String[]> _queryParams;
    private final EndPoint _endPoint;

    public UserTaskInfo(HttpServletRequest httpServletRequest,
                        List<OperationFuture> futures,
                        long startMs,
                        UUID userTaskId) {
      this(futures, httpServletRequestToString(httpServletRequest),
           KafkaCruiseControlServletUtils.getClientIpAddress(httpServletRequest), startMs, userTaskId,
           httpServletRequest.getParameterMap(), KafkaCruiseControlServletUtils.endPoint(httpServletRequest));
    }

    public UserTaskInfo(List<OperationFuture> futures,
                        String requestUrl,
                        String clientIdentity,
                        long startMs,
                        UUID userTaskId,
                        Map<String, String[]> queryParams,
                        EndPoint endPoint) {
      _futures = futures;
      _requestUrl = requestUrl;
      _clientIdentity = clientIdentity;
      _startMs = startMs;
      _userTaskId = userTaskId;
      _queryParams = queryParams;
      _endPoint = endPoint;
    }

    public List<OperationFuture> futures() {
      return _futures;
    }

    public String requestUrl() {
      return _requestUrl;
    }

    public String clientIdentity() {
      return _clientIdentity;
    }

    public long startMs() {
      return _startMs;
    }

    public UUID userTaskId() {
      return _userTaskId;
    }

    public Map<String, String[]> queryParams() {
      return _queryParams;
    }

    public EndPoint endPoint() {
      return _endPoint;
    }

    public long executionTimeNs() {
      return _futures.get(_futures.size() - 1).finishTimeNs() - TimeUnit.MILLISECONDS.toNanos(_startMs);
    }

    public String requestWithParams() {
      StringBuilder sb = new StringBuilder(_requestUrl);
      String queryParamDelimiter = "?";
      for (Map.Entry<String, String[]> paramSet : _queryParams.entrySet()) {
        for (String paramValue : paramSet.getValue()) {
          sb.append(queryParamDelimiter).append(paramSet.getKey()).append("=").append(paramValue);
          if (queryParamDelimiter.equals("?")) {
            queryParamDelimiter = "&";
          }
        }
      }
      return  sb.toString();
    }
  }

  /**
   * A runnable class to remove expired session, completed user tasks and old inactive tasks.
   */
  private class UserTaskScanner implements Runnable {
    @Override
    public void run() {
      try {
        expireOldSessions();
        checkActiveUserTasks();
        removeOldUserTasks();
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to expire sessions.", t);
      }
    }
  }
}
