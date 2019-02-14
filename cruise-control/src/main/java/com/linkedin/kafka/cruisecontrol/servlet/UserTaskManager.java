/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlResponse;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
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
  private final Map<SessionKey, UUID> _sessionKeyToUserTaskIdMap;
  private final Map<TaskState, Map<UUID, UserTaskInfo>> _allUuidToUserTaskInfoMap;
  private final long _sessionExpiryMs;
  private final long _maxActiveUserTasks;
  private final Time _time;
  private final ScheduledExecutorService _userTaskScannerExecutor =
      Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("UserTaskScanner", true, null));
  private final UUIDGenerator _uuidGenerator;
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer;
  private final long _completedUserTaskRetentionTimeMs;

  public UserTaskManager(long sessionExpiryMs,
                         long maxActiveUserTasks,
                         long completedUserTaskRetentionTimeMs,
                         int maxCachedCompletedUserTasks,
                         MetricRegistry dropwizardMetricRegistry,
                         Map<EndPoint, Timer> successfulRequestExecutionTimer) {
    _sessionKeyToUserTaskIdMap = new HashMap<>();
    Map<UUID, UserTaskInfo> activeUserTaskIdToFuturesMap = new LinkedHashMap<>();
    // completedUserTaskIdToFuturesMap stores tasks completed either successfully or exceptionally.
    Map<UUID, UserTaskInfo> completedUserTaskIdToFuturesMap = new LinkedHashMap<UUID, UserTaskInfo>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<UUID, UserTaskInfo> eldest) {
        return this.size() > maxCachedCompletedUserTasks;
      }
    };
    _allUuidToUserTaskInfoMap = new HashMap<>(2);
    _allUuidToUserTaskInfoMap.put(TaskState.ACTIVE, activeUserTaskIdToFuturesMap);
    _allUuidToUserTaskInfoMap.put(TaskState.COMPLETED, completedUserTaskIdToFuturesMap);

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
                                      (Gauge<Integer>) _sessionKeyToUserTaskIdMap::size);
    dropwizardMetricRegistry.register(MetricRegistry.name("UserTaskManager", "num-active-user-tasks"),
                                      (Gauge<Integer>) _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE)::size);
    _successfulRequestExecutionTimer = successfulRequestExecutionTimer;
  }

  // for unit-tests only
  UserTaskManager(long sessionExpiryMs,
                  long maxActiveUserTasks,
                  long completedUserTaskRetentionTimeMs,
                  int maxCachedCompletedUserTasks,
                  Time time,
                  UUIDGenerator uuidGenerator) {
    _sessionKeyToUserTaskIdMap = new HashMap<>();
    Map<UUID, UserTaskInfo> activeUserTaskIdToFuturesMap = new LinkedHashMap<>();
    Map<UUID, UserTaskInfo> completedUserTaskIdToFuturesMap = new LinkedHashMap<UUID, UserTaskInfo>() {
      @Override
      protected boolean removeEldestEntry(Map.Entry<UUID, UserTaskInfo> eldest) {
        return this.size() > maxCachedCompletedUserTasks;
      }
    };
    _allUuidToUserTaskInfoMap = new HashMap<>(2);
    _allUuidToUserTaskInfoMap.put(TaskState.ACTIVE, activeUserTaskIdToFuturesMap);
    _allUuidToUserTaskInfoMap.put(TaskState.COMPLETED, completedUserTaskIdToFuturesMap);

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
    EndPoint.cachedValues().forEach(e -> _successfulRequestExecutionTimer.put(e, new Timer()));
  }

  // for unit-tests only
  UserTaskManager(long sessionExpiryMs,
                  long maxActiveUserTasks,
                  long completedUserTaskRetentionTimeMs,
                  int maxCachedCompletedUserTasks,
                  Time time) {
    this(sessionExpiryMs, maxActiveUserTasks, completedUserTaskRetentionTimeMs, maxCachedCompletedUserTasks, time, new UUIDGenerator());
  }

  private static String httpServletRequestToString(HttpServletRequest request) {
    return String.format("%s %s", request.getMethod(), request.getRequestURI());
  }

  /**
   * Create the Asynchronous UserTask reference if it doesn't exist.
   *
   * This method creates references {@link UserTaskInfo} and maps it to {@link HttpSession} from httpServletRequest. The
   * {@link HttpSession} is also used to fetch {@link OperationFuture} for the in-progress/completed UserTask. If the
   * UserTaskID is passed in the httpServletRequest header then that takes precedence over {@link HttpSession} to fetch
   * the {@link OperationFuture} for the UserTask.
   *
   * @param httpServletRequest the HttpServletRequest to create the UserTaskInfo reference.
   * @param httpServletResponse the HttpServletResponse that contains the UserTaskId in the HttpServletResponse header.
   * @param function A function that takes a UUID and returns {@link OperationFuture}.
   * @param step The index of the step that has to be added or fetched.
   * @return The list of {@link OperationFuture} for the linked UserTask.
   */
  public List<OperationFuture> getOrCreateAsyncUserTask(HttpServletRequest httpServletRequest,
                                                        HttpServletResponse httpServletResponse,
                                                        Function<String, OperationFuture> function,
                                                        int step) {
    UUID userTaskId = getUserTaskId(httpServletRequest);

    AsyncUserTaskInfo userTaskInfo = (AsyncUserTaskInfo) getUserTaskByUserTaskId(userTaskId, httpServletRequest);
    Function<UUID, Consumer<UserTaskInfo>> consumer = (uuid) -> {
      return (userTask) -> {
        OperationFuture future = function.apply(uuid.toString());
        userTask.futures().add(future);
      };
    };

    if (userTaskInfo != null) {
      LOG.info("Fetch an existing UserTask {}", userTaskId);
      httpServletResponse.setHeader(USER_TASK_HEADER_NAME, userTaskId.toString());
      if (step < userTaskInfo.futures().size()) {
        return userTaskInfo.futures();
      } else if (step == userTaskInfo.futures().size()) {
        LOG.info("Add a new future to existing UserTask {}", userTaskId);
        return insertUserTaskById(userTaskId, consumer.apply(userTaskId), httpServletRequest, false).futures();
      } else {
        throw new IllegalArgumentException(
            String.format("There are %d steps in the session. Cannot add step %d.", userTaskInfo.futures().size(), step));
      }
    } else {


      ensureUserTaskHeaderNotPresent(USER_TASK_HEADER_NAME, httpServletRequest);
      if (step != 0) {
        throw new IllegalArgumentException(
            String.format("There are no step in the session. Cannot add step %d.", step));
      }
      UUID userTaskId2 = _uuidGenerator.randomUUID();
      userTaskInfo = (AsyncUserTaskInfo) insertUserTaskById(userTaskId2, consumer.apply(userTaskId2), httpServletRequest, false);
      createSessionKeyMapping(userTaskId2, httpServletRequest);


      httpServletResponse.setHeader(USER_TASK_HEADER_NAME, userTaskId2.toString());
      return userTaskInfo.futures();
    }
  }

  /**
   * Create the Synchronous UserTask reference if it doesn't exist.
   *
   * For association between SyncUserTaskInfo with UUID and sessionKey, see description above getOrCreateAsyncUserTask. The difference
   * between Synchronous and Asynchronous User Task is that synchronous tasks are do not have in-progress state. They are created when
   * a synchronous request is finished, and inserted into _allUuidToUserTaskInfoMap for record keeping purpose.
   *
   * @param httpServletRequest the HttpServletRequest to create the UserTaskInfo reference.
   * @param httpServletResponse the HttpServletResponse that contains the UserTaskId in the HttpServletResponse header.
   * @return CruiseControlResponse the result of execution of synchronous tasks. Note that not all tasks have results (e.g. pauseSampling)
   * for those cases, return null.
   */
  public CruiseControlResponse getOrCreateSyncUserTask(HttpServletRequest httpServletRequest,
                                                       HttpServletResponse httpServletResponse,
                                                       Supplier<CruiseControlResponse> supplier) {
    UUID userTaskId = getUserTaskId(httpServletRequest);

    SyncUserTaskInfo userTaskInfo = (SyncUserTaskInfo) getUserTaskByUserTaskId(userTaskId, httpServletRequest);
    if (userTaskInfo != null) {
      LOG.info("UserTask with Id {} already exists", userTaskId);
      httpServletResponse.setHeader(USER_TASK_HEADER_NAME, userTaskId.toString());

      return userTaskInfo.result();
    }

    ensureUserTaskHeaderNotPresent(USER_TASK_HEADER_NAME, httpServletRequest);

    userTaskId = _uuidGenerator.randomUUID();
    userTaskInfo = (SyncUserTaskInfo) insertUserTaskById(userTaskId, (userTask) -> {
      ((SyncUserTaskInfo) userTask).setResult(supplier.get());

      ((SyncUserTaskInfo) userTask).setEndMs(_time.milliseconds());
    }, httpServletRequest, true);
    createSessionKeyMapping(userTaskId, httpServletRequest);

    httpServletResponse.setHeader(USER_TASK_HEADER_NAME, userTaskId.toString());
    return userTaskInfo.result();
  }

  private void ensureUserTaskHeaderNotPresent(String headerName, HttpServletRequest httpServletRequest) {
    if (httpServletRequest.getHeader(headerName) != null) {
      // request provides user_task_header that user tasks doesn't exist then just throw exception
      String userTaskIdFromRequest = httpServletRequest.getHeader(headerName);
      throw new IllegalArgumentException(
          String.format("UserTask %s is an invalid %s", userTaskIdFromRequest, headerName));
    }
  }

  private void createSessionKeyMapping(UUID userTaskId, HttpServletRequest httpServletRequest) {
    SessionKey sessionKey = new SessionKey(httpServletRequest);
    LOG.info("Create a new UserTask {} with SessionKey {}", userTaskId, sessionKey);
    synchronized (_sessionKeyToUserTaskIdMap) {
      _sessionKeyToUserTaskIdMap.put(sessionKey, userTaskId);
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T getFuture(HttpServletRequest request) {
    UUID userTaskId = getUserTaskId(request);
    UserTaskInfo userTaskInfo = getUserTaskByUserTaskId(userTaskId, request);
    List<OperationFuture> operationFutures = null;
    if (userTaskInfo != null) {
      operationFutures = userTaskInfo.futures();
    }
    if (operationFutures == null || operationFutures.isEmpty()) {
      return null;
    }

    return (T) operationFutures.get(operationFutures.size() - 1);
  }

  private void expireOldSessions() {
    long now = _time.milliseconds();
    synchronized (_sessionKeyToUserTaskIdMap) {
      Iterator<Map.Entry<SessionKey, UUID>> iter = _sessionKeyToUserTaskIdMap.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<SessionKey, UUID> entry = iter.next();
        SessionKey sessionKey = entry.getKey();
        HttpSession session = sessionKey.httpSession();
        try {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Session {} was last accessed at {}, age is {} ms.", session, session.getLastAccessedTime(),
                      now - session.getLastAccessedTime());
          }
          if (now >= session.getLastAccessedTime() + _sessionExpiryMs) {
            LOG.info("Expiring the session associated with {}.", sessionKey);
            session.invalidate();
            iter.remove();
          }
        } catch (IllegalStateException e) {
          LOG.info("Already expired the session associated with {}.", sessionKey);
          iter.remove();
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
      synchronized (_sessionKeyToUserTaskIdMap) {
        userTaskId = _sessionKeyToUserTaskIdMap.get(sessionKey);
      }
    }

    return userTaskId;
  }

   synchronized void checkActiveUserTasks() {
    Iterator<Map.Entry<UUID, UserTaskInfo>> iter = _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<UUID, UserTaskInfo> entry = iter.next();
      if (entry.getValue().isUserTaskDoneExceptionally()) {
        LOG.warn("UserTask {} is completed with Exception and removed from active tasks list", entry.getKey());
        _allUuidToUserTaskInfoMap.get(TaskState.COMPLETED).put(entry.getKey(), entry.getValue().setState(TaskState.COMPLETED_WITH_ERROR));
        iter.remove();
      } else if (entry.getValue().isUserTaskDone()) {
        LOG.info("UserTask {} is completed and removed from active tasks list", entry.getKey());
        _successfulRequestExecutionTimer.get(entry.getValue().endPoint()).update(entry.getValue().executionTimeNs(), TimeUnit.NANOSECONDS);
        _allUuidToUserTaskInfoMap.get(TaskState.COMPLETED).put(entry.getKey(), entry.getValue().setState(TaskState.COMPLETED));
        iter.remove();
      }
    }
  }

  private synchronized void removeOldUserTasks() {
    LOG.debug("Remove old user tasks");
    _allUuidToUserTaskInfoMap.get(TaskState.COMPLETED).entrySet().removeIf(entry -> (entry.getValue().startMs()
                                                                                     + _completedUserTaskRetentionTimeMs < _time.milliseconds()));
  }

  synchronized UserTaskInfo getUserTaskByUserTaskId(UUID userTaskId, HttpServletRequest httpServletRequest) {
    if (userTaskId == null) {
      return null;
    }

    String requestUrl = httpServletRequestToString(httpServletRequest);
    if (_allUuidToUserTaskInfoMap.get(TaskState.COMPLETED).containsKey(userTaskId)) {
      UserTaskInfo userTaskInfo = _allUuidToUserTaskInfoMap.get(TaskState.COMPLETED).get(userTaskId);
      if (userTaskInfo.requestUrl().equals(requestUrl)
          && hasTheSameHttpParameter(userTaskInfo.queryParams(), httpServletRequest.getParameterMap())) {
        return userTaskInfo;
      }
    }

    if (_allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).containsKey(userTaskId)) {
      UserTaskInfo userTaskInfo = _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).get(userTaskId);
      if (userTaskInfo.requestUrl().equals(requestUrl)
          && hasTheSameHttpParameter(userTaskInfo.queryParams(), httpServletRequest.getParameterMap())) {
        return userTaskInfo;
      }
    }

    return null;
  }

  /**
   * Common function for adding sync/async tasks
   * @param userTaskId UUID to uniquely identify task.
   * @param operation lambda function to provide result to UserTaskInfo object
   * @param httpServletRequest
   * @param isSync: indicate whether the task created is sync or async
   * @return
   */
  private synchronized UserTaskInfo insertUserTaskById(UUID userTaskId,
                                                       Consumer<UserTaskInfo> operation,
                                                       HttpServletRequest httpServletRequest,
                                                       boolean isSync) {
    if (_allUuidToUserTaskInfoMap.get(TaskState.COMPLETED).containsKey(userTaskId)) {
      // Before add new operation to task, first recycle the task from completed task list.
      _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).put(userTaskId, _allUuidToUserTaskInfoMap.get(TaskState.COMPLETED)
          .remove(userTaskId).setState(TaskState.ACTIVE));
      LOG.info("UserTask {} is recycled from complete task list and added back to active tasks list", userTaskId);
    }
    UserTaskInfo userTaskInfo;

    if (_allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).containsKey(userTaskId)) {
      userTaskInfo = _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).get(userTaskId);
      operation.accept(userTaskInfo);
    } else {
      if (_allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).size() >= _maxActiveUserTasks) {
        throw new RuntimeException("There are already " + _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).size() +
                                   " active user tasks, which has reached the servlet capacity.");
      }
      if (isSync) {
        userTaskInfo = new SyncUserTaskInfo(httpServletRequest, _time.milliseconds(), userTaskId, TaskState.ACTIVE);
      } else {
        userTaskInfo = new AsyncUserTaskInfo(httpServletRequest, _time.milliseconds(), userTaskId, TaskState.ACTIVE);
      }
      operation.accept(userTaskInfo);
      _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).put(userTaskId, userTaskInfo);
    }
    return _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).get(userTaskId);
  }

  public synchronized List<UserTaskInfo> getActiveUserTasks() {
    return new ArrayList<>(_allUuidToUserTaskInfoMap.get(TaskState.ACTIVE).values());
  }

  public synchronized List<UserTaskInfo> getCompletedUserTasks() {
    return new ArrayList<>(_allUuidToUserTaskInfoMap.get(TaskState.COMPLETED).values());
  }

  @Override
  public String toString() {
    Map<UUID, UserTaskInfo> completedUserTaskIdToFuturesMap = new LinkedHashMap<>();
    Map<UUID, UserTaskInfo> completedWithErrorUserTaskIdToFuturesMap = new LinkedHashMap<>();
    _allUuidToUserTaskInfoMap.get(TaskState.COMPLETED).forEach((k, v) -> {
      if (v.state() == TaskState.COMPLETED) {
        completedUserTaskIdToFuturesMap.put(k, v);
      } else {
        completedWithErrorUserTaskIdToFuturesMap.put(k, v);
      }
    });
    return "UserTaskManager{_sessionKeyToUserTaskIdMap=" + _sessionKeyToUserTaskIdMap
           + ", _activeUserTaskIdToFuturesMap=" + _allUuidToUserTaskInfoMap.get(TaskState.ACTIVE)
           + ", _completedUserTaskIdToFuturesMap=" + _allUuidToUserTaskInfoMap.get(TaskState.COMPLETED)
           + ", _completedWithErrorUserTaskIdToFuturesMap=" + completedWithErrorUserTaskIdToFuturesMap + '}';
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
  int numActiveSessionKeys() {
    return _sessionKeyToUserTaskIdMap.size();
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
  public abstract static class UserTaskInfo {
    private final String _requestUrl;
    private final String _clientIdentity;
    protected final long _startMs;
    private final UUID _userTaskId;
    private final Map<String, String[]> _queryParams;
    private final EndPoint _endPoint;
    private TaskState _state;

    public UserTaskInfo(HttpServletRequest httpServletRequest,
                        long startMs,
                        UUID userTaskId,
                        TaskState state) {
      this(httpServletRequestToString(httpServletRequest),
           KafkaCruiseControlServletUtils.getClientIpAddress(httpServletRequest), startMs, userTaskId,
           httpServletRequest.getParameterMap(), ParameterUtils.endPoint(httpServletRequest), state);
    }

    public UserTaskInfo(String requestUrl,
                        String clientIdentity,
                        long startMs,
                        UUID userTaskId,
                        Map<String, String[]> queryParams,
                        EndPoint endPoint,
                        TaskState state) {

      _requestUrl = requestUrl;
      _clientIdentity = clientIdentity;
      _startMs = startMs;
      _userTaskId = userTaskId;
      _queryParams = queryParams;
      _endPoint = endPoint;
      _state = state;
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


    public abstract long executionTimeNs();
    public abstract List<OperationFuture> futures();
    public abstract CruiseControlResponse result();
    public abstract boolean isUserTaskDone();

    public TaskState state() {
      return _state;
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

    public UserTaskInfo setState(TaskState nextState) {
      _state = nextState;
      return this;
    }

    boolean isUserTaskDone() {
      return _futures.get(_futures.size() - 1).isDone();
    }

    boolean isUserTaskDoneExceptionally() {
      return _futures.get(_futures.size() - 1).isCompletedExceptionally();
    }
  }

  public static class AsyncUserTaskInfo extends UserTaskInfo {
    private final List<OperationFuture> _futures;
    public AsyncUserTaskInfo(HttpServletRequest httpServletRequest,
                            long startMs,
                            UUID userTaskId,
                            TaskState state) {
      super(httpServletRequest, startMs, userTaskId, state);
      _futures = new ArrayList<>();
    }

    public List<OperationFuture> futures() {
      return _futures;
    }

    public CruiseControlResponse result() {
      return null;
    }

    @Override
    public long executionTimeNs() {
      return _futures.get(_futures.size() - 1).finishTimeNs() - TimeUnit.MILLISECONDS.toNanos(_startMs);
    }

    @Override
    public boolean isUserTaskDone() {
      return _futures == null || _futures.isEmpty();
    }
  }

  public static class SyncUserTaskInfo extends UserTaskInfo {
      private CruiseControlResponse _result;
      private long _endMs;
      public SyncUserTaskInfo(HttpServletRequest httpServletRequest,
                          long startMs,
                          UUID userTaskId,
                          TaskState state) {
        super(httpServletRequest, startMs, userTaskId, state);
      }

      // Synchronous tasks do not use Futures
      public List<OperationFuture> futures() {
        return null;
      }

      public CruiseControlResponse result() {
        return _result;
      }

      public void setResult(CruiseControlResponse result) {
        _result = result;
      }

      public void setEndMs(long endMs) {
        _endMs = endMs;
      }

      @Override
      public long executionTimeNs() {
        return TimeUnit.MILLISECONDS.toNanos(_endMs - _startMs);
      }

      // Sync task are done when they are inserted.
      @Override
      public boolean isUserTaskDone() {
        return true;
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
   * Possible state of tasks. We currently accept {@link TaskState#ACTIVE} and {@link TaskState#COMPLETED}, in the
   * future we could also add Cancelled state.
   */
  public enum TaskState {
    ACTIVE("Active"),
    COMPLETED("Completed"),
    COMPLETED_WITH_ERROR("CompletedWithError");

    private String _type;
    TaskState(String type) {
      _type = type;
    }

    public String type() {
      return _type;
    }

    @Override
    public String toString() {
      return _type;
    }

    private static final List<TaskState> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(ACTIVE, COMPLETED));

    /**
     * Use this instead of values() because values() creates a new array each time.
     * @return enumerated values in the same order as values()
     */
    public static List<TaskState> cachedValues() {
      return CACHED_VALUES;
    }
  }
}
