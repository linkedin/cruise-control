/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * {@link UserTaskManager} keeps track of long running request.
 *
 * {@link HttpServletRequest} can execute for long durations. The servlet submits the asynchronous tasks and return the
 * progress of the operation instead of blocking for the operation to complete. {@link UserTaskManager} maintains the
 * mapping of request's {@link HttpSession} -> UserTaskID({@link UUID}). The same client can issue the same request
 * again and the {@link UserTaskManager} can return the status of the task execution. A client can also get the status
 * of the execution using the UserTaskId {@link UUID}.
 *
 * Cruise Control uses {@link HttpSession} to man
 */
public class UserTaskManager implements Closeable {
  public static final String USER_TASK_HEADER_NAME = "User-Task-ID";
  public static final long USER_TASK_SCANNER_PERIOD_SECONDS = 5;

  private static final Logger LOG = LoggerFactory.getLogger(UserTaskManager.class);
  private final Map<SessionKey, UUID> _sessionToUserTaskIdMap;
  private final Map<UUID, UserTaskInfo> _activeUserTaskIdToFuturesMap;
  private final Map<UUID, UserTaskInfo> _userTaskIdToFuturesMap;
  private final long _sessionExpiryMs;
  private final long _maxActiveUserTasks;
  private final Time _time;
  private final ScheduledExecutorService _userTaskScannerExecutor =
      Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("UserTaskScanner", true, null));
  private final UUIDGenerator _uuidGenerator;

  public UserTaskManager(long sessionExpiryMs, long maxActiveUserTasks, MetricRegistry dropwizardMetricRegistry) {
    this(new HashMap<>(), new HashMap<>(), new HashMap<>(), sessionExpiryMs, maxActiveUserTasks,
        dropwizardMetricRegistry);
  }

  private UserTaskManager(Map<SessionKey, UUID> sessionToUserTaskIdMap,
      Map<UUID, UserTaskInfo> activeUserTaskIdToFuturesMap, Map<UUID, UserTaskInfo> userTaskIdToFuturesMap,
      long sessionExpiryMs, long maxActiveUserTasks, MetricRegistry dropwizardMetricRegistry) {
    _sessionToUserTaskIdMap = sessionToUserTaskIdMap;
    _activeUserTaskIdToFuturesMap = activeUserTaskIdToFuturesMap;
    _userTaskIdToFuturesMap = userTaskIdToFuturesMap;
    _sessionExpiryMs = sessionExpiryMs;
    _maxActiveUserTasks = maxActiveUserTasks;
    _time = Time.SYSTEM;
    _uuidGenerator = new UUIDGenerator();
    _userTaskScannerExecutor.scheduleAtFixedRate(new UserTaskScanner(), 0, USER_TASK_SCANNER_PERIOD_SECONDS,
        TimeUnit.SECONDS);
    dropwizardMetricRegistry.register(MetricRegistry.name("UserTaskManager", "num-active-sessions"),
        (Gauge<Integer>) _sessionToUserTaskIdMap::size);
    dropwizardMetricRegistry.register(MetricRegistry.name("UserTaskManager", "num-active-user-tasks"),
        (Gauge<Integer>) _activeUserTaskIdToFuturesMap::size);
  }

  // for unit-tests only
  UserTaskManager(long sessionExpiryMs, long maxActiveUserTasks, Time time, UUIDGenerator uuidGenerator) {
    _sessionToUserTaskIdMap = new HashMap<>();
    _activeUserTaskIdToFuturesMap = new HashMap<>();
    _userTaskIdToFuturesMap = new HashMap<>();
    _sessionExpiryMs = sessionExpiryMs;
    _maxActiveUserTasks = maxActiveUserTasks;
    _time = time;
    _uuidGenerator = uuidGenerator;
    _userTaskScannerExecutor.scheduleAtFixedRate(new UserTaskScanner(), 0, 5, TimeUnit.SECONDS);
  }

  // for unit-tests only
  UserTaskManager(long sessionExpiryMs, long maxActiveUserTasks, Time time) {
    _sessionToUserTaskIdMap = new HashMap<>();
    _activeUserTaskIdToFuturesMap = new HashMap<>();
    _userTaskIdToFuturesMap = new HashMap<>();
    _sessionExpiryMs = sessionExpiryMs;
    _maxActiveUserTasks = maxActiveUserTasks;
    _time = time;
    _uuidGenerator = new UUIDGenerator();
    ;
    _userTaskScannerExecutor.scheduleAtFixedRate(new UserTaskScanner(), 0, 5, TimeUnit.SECONDS);
  }

  private static String httpServletRequestToString(HttpServletRequest request) {
    return String.format("%s(%s %s)", request.getClass().getSimpleName(), request.getMethod(), request.getRequestURI());
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
      HttpServletResponse httpServletResponse, Supplier<OperationFuture<T>> operation, int step) {
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
      if (step > 0) {
        throw new IllegalArgumentException(
            String.format("There are no step in the session. Cannot add step %d.", step));
      }

      HttpSession httpSession = httpServletRequest.getSession(false);
      if (httpSession == null) { // create session if session does not exist
        httpServletRequest.getSession(true);
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

  public void closeSession(HttpServletRequest request) {
    SessionKey sessionKey = new SessionKey(request);
    UUID userTaskId;
    synchronized (_sessionToUserTaskIdMap) {
      userTaskId = _sessionToUserTaskIdMap.remove(sessionKey);
    }

    if (userTaskId != null) {
      LOG.info("Closing SessionKey {} and UserTaskId {}", sessionKey, userTaskId);
    }

    if (userTaskId != null && isActiveUserTasksDone(userTaskId)) {
      LOG.info("Invalidate SessionKey {}", sessionKey);
      sessionKey._httpSession.invalidate();
    }
  }

  private void expireOldSessions() {
    long now = _time.milliseconds();
    synchronized (_sessionToUserTaskIdMap) {
      Iterator<Map.Entry<SessionKey, UUID>> iter = _sessionToUserTaskIdMap.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<SessionKey, UUID> entry = iter.next();
        SessionKey sessionKey = entry.getKey();
        HttpSession session = sessionKey._httpSession;
        LOG.trace("Session {} was last accessed at {}, age is {} ms", session, session.getLastAccessedTime(),
            now - session.getLastAccessedTime());
        if (now >= session.getLastAccessedTime() + _sessionExpiryMs) {
          LOG.info("Expiring SessionKey {}", entry.getKey());
          iter.remove();
          session.invalidate();
          // NOTE: Does it make sense to cancel the future?
        }
      }
    }
  }

  private UUID getUserTaskId(HttpServletRequest httpServletRequest) {
    SessionKey sessionKey = new SessionKey(httpServletRequest);
    String userTaskIdString = httpServletRequest.getHeader(USER_TASK_HEADER_NAME);

    UUID userTaskId;
    if (userTaskIdString != null && !userTaskIdString.isEmpty()) { // valid user task id
      userTaskId = UUID.fromString(userTaskIdString);
    } else {
      synchronized (_sessionToUserTaskIdMap) {
        userTaskId = _sessionToUserTaskIdMap.get(sessionKey);
      }
    }

    return userTaskId;
  }

  private synchronized boolean isActiveUserTasksDone(UUID userTaskId) {
    UserTaskInfo userTaskInfo = _activeUserTaskIdToFuturesMap.get(userTaskId);
    if (userTaskInfo == null || userTaskInfo.getFutures().isEmpty()) {
      return true;
    }

    List<OperationFuture> futures = userTaskInfo.getFutures();
    return futures.get(futures.size() - 1).isDone();
  }

  private synchronized void checkActiveUserTasks() {
    Iterator<Map.Entry<UUID, UserTaskInfo>> iter = _activeUserTaskIdToFuturesMap.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<UUID, UserTaskInfo> entry = iter.next();
      if (isActiveUserTasksDone(entry.getKey())) {
        LOG.info("UserTask {} is complete and removed from active tasks list", entry.getKey());
        _userTaskIdToFuturesMap.put(entry.getKey(), entry.getValue());
        iter.remove();
      }
    }
  }

  private synchronized void removeOldUserTasks() {
    LOG.info("Remove inactive tasks");
    _userTaskIdToFuturesMap.entrySet()
        .removeIf(entry -> (entry.getValue().getStartMs() + TimeUnit.HOURS.toMillis(6) < _time.milliseconds()));
  }

  synchronized List<OperationFuture> getFuturesByUserTaskId(UUID userTaskId, HttpServletRequest httpServletRequest) {
    if (userTaskId == null) {
      return null;
    }

    String requestUrl = httpServletRequestToString(httpServletRequest);
    if (_userTaskIdToFuturesMap.containsKey(userTaskId)) {
      UserTaskInfo userTaskInfo = _userTaskIdToFuturesMap.get(userTaskId);
      if (userTaskInfo.getRequestUrl().equals(requestUrl)) {
        return userTaskInfo.getFutures();
      }
    }

    if (_activeUserTaskIdToFuturesMap.containsKey(userTaskId)) {
      UserTaskInfo userTaskInfo = _activeUserTaskIdToFuturesMap.get(userTaskId);
      if (userTaskInfo.getRequestUrl().equals(requestUrl)) {
        return userTaskInfo.getFutures();
      }
    }

    return null;
  }

  private synchronized void insertFuturesByUserTaskId(UUID userTaskId, OperationFuture operationFuture,
      HttpServletRequest httpServletRequest) {
    if (_activeUserTaskIdToFuturesMap.containsKey(userTaskId)) {
      _activeUserTaskIdToFuturesMap.get(userTaskId).getFutures().add(operationFuture);
    } else {
      if (_activeUserTaskIdToFuturesMap.size() >= _maxActiveUserTasks) {
        throw new RuntimeException(
            "There are already " + _activeUserTaskIdToFuturesMap.size() + " active user task, which "
                + "has reached the servlet capacity.");
      }
      UserTaskInfo userTaskInfo =
          new UserTaskInfo(httpServletRequest, new ArrayList<>(Collections.singleton(operationFuture)),
              _time.milliseconds());
      _activeUserTaskIdToFuturesMap.put(userTaskId, userTaskInfo);
    }
  }

  public synchronized Map<UUID, UserTaskInfo> getActiveUserTasks() {
    HashMap<UUID, UserTaskInfo> activeUserTasks = new HashMap<>();
    for (Map.Entry<UUID, UserTaskInfo> userTasks : _activeUserTaskIdToFuturesMap.entrySet()) {
      UserTaskInfo userTaskInfo = userTasks.getValue();
      activeUserTasks.put(userTasks.getKey(),
          new UserTaskInfo(userTaskInfo.getRequestUrl(), userTaskInfo.getClientIdentity(), userTaskInfo.getStartMs()));
    }
    return activeUserTasks;
  }

  @Override
  public String toString() {
    return "UserTaskManager{" + "_sessionToUserTaskIdMap=" + _sessionToUserTaskIdMap
        + ", _activeUserTaskIdToFuturesMap=" + _activeUserTaskIdToFuturesMap + ", _userTaskIdToFuturesMap="
        + _userTaskIdToFuturesMap + '}';
  }

  @Override
  public void close() {
    _userTaskScannerExecutor.shutdownNow();
  }

  static private class SessionKey {
    private final HttpSession _httpSession;
    private final String _requestUrl;
    private final Map<String, String[]> _queryParams;

    SessionKey(HttpServletRequest httpServletRequest) {
      _httpSession = httpServletRequest.getSession(false);
      _requestUrl = httpServletRequestToString(httpServletRequest);
      _queryParams = httpServletRequest.getParameterMap();
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
      return Objects.equals(_httpSession, that._httpSession) && Objects.equals(_requestUrl, that._requestUrl) && Objects
          .equals(_queryParams, that._queryParams);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_httpSession, _requestUrl, _queryParams);
    }

    @Override
    public String toString() {
      return "SessionKey{" + "_httpSession=" + _httpSession + ", _requestUrl='" + _requestUrl + '\'' + ", _queryParams="
          + _queryParams + '}';
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

  /**
   * A internal class for generating random UUID for user tasks.
   */
  static public class UUIDGenerator {
    UUID randomUUID() {
      return UUID.randomUUID();
    }
  }

  /**
   * A class to encapsulate UserTask.
   */
  static public class UserTaskInfo {
    private final List<OperationFuture> _futures;
    private final String _requestUrl;
    private final String _clientIdentity;
    private final long _startMs;

    public UserTaskInfo(String requestUrl, String clientIdentity, long startMs) {
      _requestUrl = requestUrl;
      _clientIdentity = clientIdentity;
      _futures = null;
      _startMs = startMs;
    }

    public UserTaskInfo(HttpServletRequest httpServletRequest, List<OperationFuture> futures, long startMs) {
      _requestUrl = httpServletRequestToString(httpServletRequest);
      _clientIdentity = KafkaCruiseControlServletUtils.getClientIpAddress(httpServletRequest);
      _futures = futures;
      _startMs = startMs;
    }

    public List<OperationFuture> getFutures() {
      return _futures;
    }

    public String getRequestUrl() {
      return _requestUrl;
    }

    public String getClientIdentity() {
      return _clientIdentity;
    }

    public long getStartMs() {
      return _startMs;
    }
  }
}
