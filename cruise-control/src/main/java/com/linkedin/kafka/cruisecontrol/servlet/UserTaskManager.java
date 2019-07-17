/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.EndpointType;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.OPERATION_LOGGER;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.ensureHeaderNotPresent;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.httpServletRequestToString;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_ID_PARAM;

/**
 * {@link UserTaskManager} keeps track of Sync and Async user tasks. When a {@link HttpServletRequest} comes in, the servlet
 * creates the tasks to track the corresponding request and results.
 *
 * Some {@link HttpServletRequest} can execute for long durations, the servlet submits the asynchronous tasks and returns the
 * progress of the operation instead of blocking for the operation to complete. {@link UserTaskManager} maintains the
 * mapping of Request URL and {@link HttpSession} to UserTaskID({@link UUID}) for async requests. To fetch the status of an async
 * request, the client can use the same Request ULR along with session cookie to retrieve the current status. The status of the
 * request can also be fetched using the UserTaskID. '/user_tasks' endpoint can be used to fetch all the active and
 * recently completed UserTasks. For sync requests, only UserTaskID can be used to fetch their status.
 */
public class UserTaskManager implements Closeable {
  public static final String USER_TASK_HEADER_NAME = "User-Task-ID";
  public static final long USER_TASK_SCANNER_PERIOD_SECONDS = 5;
  public static final long USER_TASK_SCANNER_INITIAL_DELAY_SECONDS = 0;

  private static final Logger LOG = LoggerFactory.getLogger(UserTaskManager.class);
  private static final Logger OPERATION_LOG = LoggerFactory.getLogger(OPERATION_LOGGER);
  private final Map<SessionKey, UUID> _sessionKeyToUserTaskIdMap;
  private final Map<UUID, UserTaskInfo> _uuidToActiveUserTaskInfoMap;
  private final Map<EndpointType, Map<UUID, UserTaskInfo>> _uuidToCompletedUserTaskInfoMap;
  private UserTaskInfo _inExecutionUserTaskInfo;
  private final long _sessionExpiryMs;
  private final int _maxActiveUserTasks;
  private final Time _time;
  private final ScheduledExecutorService _userTaskScannerExecutor =
      Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("UserTaskScanner", true, null));
  private final ExecutorService _userTaskLoggerExecutor =
      Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("UserTaskLogger", true, null));
  private final UUIDGenerator _uuidGenerator;
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer;
  private final Map<EndpointType, Long> _completedUserTaskRetentionTimeMs;
  private final Purgatory _purgatory;

  public UserTaskManager(KafkaCruiseControlConfig config,
                         MetricRegistry dropwizardMetricRegistry,
                         Map<EndPoint, Timer> successfulRequestExecutionTimer,
                         Purgatory purgatory) {
    _purgatory = purgatory;
    _sessionKeyToUserTaskIdMap = new HashMap<>();
    List<CruiseControlEndpointType> endpointTypes = Collections.unmodifiableList(Arrays.asList(CruiseControlEndpointType.values()));
    _uuidToCompletedUserTaskInfoMap =  new HashMap<>(endpointTypes.size());
    _completedUserTaskRetentionTimeMs = new HashMap<>(endpointTypes.size());
    initCompletedUserTaskRetentionPolicy(config, endpointTypes);
    _sessionExpiryMs = config.getLong(KafkaCruiseControlConfig.WEBSERVER_SESSION_EXPIRY_MS);
    _maxActiveUserTasks = config.getInt(KafkaCruiseControlConfig.MAX_ACTIVE_USER_TASKS_CONFIG);
    _uuidToActiveUserTaskInfoMap = new LinkedHashMap<>(_maxActiveUserTasks);
    _time = Time.SYSTEM;
    _uuidGenerator = new UUIDGenerator();
    _userTaskScannerExecutor.scheduleAtFixedRate(new UserTaskScanner(),
                                                 USER_TASK_SCANNER_INITIAL_DELAY_SECONDS,
                                                 USER_TASK_SCANNER_PERIOD_SECONDS,
                                                 TimeUnit.SECONDS);
    dropwizardMetricRegistry.register(MetricRegistry.name("UserTaskManager", "num-active-sessions"),
                                      (Gauge<Integer>) _sessionKeyToUserTaskIdMap::size);
    dropwizardMetricRegistry.register(MetricRegistry.name("UserTaskManager", "num-active-user-tasks"),
                                      (Gauge<Integer>) _uuidToActiveUserTaskInfoMap::size);
    _successfulRequestExecutionTimer = successfulRequestExecutionTimer;
  }

  // for unit-tests only
  UserTaskManager(long sessionExpiryMs,
                  int maxActiveUserTasks,
                  long completedUserTaskRetentionTimeMs,
                  int maxCachedCompletedUserTasks,
                  Time time,
                  UUIDGenerator uuidGenerator) {
    _purgatory = null;
    _sessionKeyToUserTaskIdMap = new HashMap<>();
    _uuidToActiveUserTaskInfoMap = new LinkedHashMap<>(maxActiveUserTasks);
    List<CruiseControlEndpointType> endpointTypes = Collections.unmodifiableList(Arrays.asList(CruiseControlEndpointType.values()));
    _uuidToCompletedUserTaskInfoMap = new HashMap<>(endpointTypes.size());
    _completedUserTaskRetentionTimeMs = new HashMap<>(endpointTypes.size());
    for (CruiseControlEndpointType endpointType : endpointTypes) {
      _uuidToCompletedUserTaskInfoMap.put(endpointType, new LinkedHashMap<UUID, UserTaskInfo>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<UUID, UserTaskInfo> eldest) {
          return this.size() > maxCachedCompletedUserTasks;
        }
      });
      _completedUserTaskRetentionTimeMs.put(endpointType, completedUserTaskRetentionTimeMs);
    }
    _sessionExpiryMs = sessionExpiryMs;
    _maxActiveUserTasks = maxActiveUserTasks;
    _time = time;
    _uuidGenerator = uuidGenerator;
    _userTaskScannerExecutor.scheduleAtFixedRate(new UserTaskScanner(),
                                                 USER_TASK_SCANNER_INITIAL_DELAY_SECONDS,
                                                 USER_TASK_SCANNER_PERIOD_SECONDS,
                                                 TimeUnit.SECONDS);
    _successfulRequestExecutionTimer = new HashMap<>();
    CruiseControlEndPoint.cachedValues().forEach(e -> _successfulRequestExecutionTimer.put(e, new Timer()));
  }

  // for unit-tests only
  UserTaskManager(long sessionExpiryMs,
                  int maxActiveUserTasks,
                  long completedUserTaskRetentionTimeMs,
                  int maxCachedCompletedUserTasks,
                  Time time) {
    this(sessionExpiryMs, maxActiveUserTasks, completedUserTaskRetentionTimeMs, maxCachedCompletedUserTasks, time, new UUIDGenerator());
  }

  private void initCompletedUserTaskRetentionPolicy(KafkaCruiseControlConfig config, List<CruiseControlEndpointType> endpointTypes) {
    Integer defaultMaxCachedCompletedUserTasks = config.getInt(KafkaCruiseControlConfig.MAX_CACHED_COMPLETED_USER_TASKS_CONFIG);
    Long defaultCompletedUserTaskRetentionTimeMs = config.getLong(KafkaCruiseControlConfig.COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG);
    for (CruiseControlEndpointType endpointType : endpointTypes) {
      Integer maxCachedCompletedUserTasks;
      Long completedUserTaskRetentionTimeMs;
      switch (endpointType) {
        case CRUISE_CONTROL_ADMIN:
          maxCachedCompletedUserTasks =
              config.getInt(KafkaCruiseControlConfig.MAX_CACHED_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASKS_CONFIG);
          completedUserTaskRetentionTimeMs =
              config.getLong(KafkaCruiseControlConfig.COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASK_RETENTION_TIME_MS_CONFIG);
          break;
        case KAFKA_ADMIN:
          maxCachedCompletedUserTasks =
              config.getInt(KafkaCruiseControlConfig.MAX_CACHED_COMPLETED_KAFKA_ADMIN_USER_TASKS_CONFIG);
          completedUserTaskRetentionTimeMs =
              config.getLong(KafkaCruiseControlConfig.COMPLETED_KAFKA_ADMIN_USER_TASK_RETENTION_TIME_MS_CONFIG);
          break;
        case CRUISE_CONTROL_MONITOR:
          maxCachedCompletedUserTasks =
              config.getInt(KafkaCruiseControlConfig.MAX_CACHED_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASKS_CONFIG);
          completedUserTaskRetentionTimeMs =
              config.getLong(KafkaCruiseControlConfig.COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASK_RETENTION_TIME_MS_CONFIG);
          break;
        case KAFKA_MONITOR:
          maxCachedCompletedUserTasks =
              config.getInt(KafkaCruiseControlConfig.MAX_CACHED_COMPLETED_KAFKA_MONITOR_USER_TASKS_CONFIG);
          completedUserTaskRetentionTimeMs =
              config.getLong(KafkaCruiseControlConfig.COMPLETED_KAFKA_MONITOR_USER_TASK_RETENTION_TIME_MS_CONFIG);
          break;
        default:
          throw new IllegalStateException("Unknown endpoint type " + endpointType);
      }
      Integer mapSize = maxCachedCompletedUserTasks == null ? defaultMaxCachedCompletedUserTasks : maxCachedCompletedUserTasks;
      _uuidToCompletedUserTaskInfoMap.put(endpointType, new LinkedHashMap<UUID, UserTaskInfo>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<UUID, UserTaskInfo> eldest) {
          return this.size() > mapSize;
        }
      });
      _completedUserTaskRetentionTimeMs.put(endpointType,
          completedUserTaskRetentionTimeMs == null ? defaultCompletedUserTaskRetentionTimeMs : completedUserTaskRetentionTimeMs);
    }
  }

  /**
   * This method creates a {@link UserTaskInfo} reference for a new sync or async request and a UUID to map to it. For async request
   * a {@link SessionKey} is also created to map to the UUID. For async request, both UUID and {@link HttpSession} can be used to fetch
   * {@link OperationFuture} for the in-progress/completed UserTask. If the UserTaskId is passed in the httpServletRequest
   * header then that takes precedence over {@link HttpSession}. For sync task, only UUID can be used to fetch {@link OperationFuture},
   * which already contains the finished result.
   *
   * @param httpServletRequest the HttpServletRequest to create the {@link UserTaskInfo} reference.
   * @param httpServletResponse the HttpServletResponse that contains the UserTaskId in the HttpServletResponse header.
   * @param function A function that takes a UUID and returns {@link OperationFuture}. For sync task, the function always
   *                 returns a completed Future.
   * @param step The index of the step that has to be added or fetched.
   * @param isAsyncRequest Indicate whether the task is async or sync.
   * @param parameters Parsed parameters from http request, or null if the parsing result is unavailable.
   * @return An unmodifiable list of {@link OperationFuture} for the linked UserTask.
   */
  public List<OperationFuture> getOrCreateUserTask(HttpServletRequest httpServletRequest,
                                                   HttpServletResponse httpServletResponse,
                                                   Function<String, OperationFuture> function,
                                                   int step,
                                                   boolean isAsyncRequest,
                                                   CruiseControlParameters parameters) {
    UUID userTaskId = getUserTaskId(httpServletRequest);
    UserTaskInfo userTaskInfo = getUserTaskByUserTaskId(userTaskId, httpServletRequest);

    if (userTaskInfo != null) {
      LOG.info("Fetch an existing UserTask {}", userTaskId);
      httpServletResponse.setHeader(USER_TASK_HEADER_NAME, userTaskId.toString());
      if (step < userTaskInfo.futures().size()) {
        return Collections.unmodifiableList(userTaskInfo.futures());
      } else if (step == userTaskInfo.futures().size()) {
        LOG.info("Add a new future to existing UserTask {}", userTaskId);
        return Collections.unmodifiableList(insertFuturesByUserTaskId(userTaskId, function, httpServletRequest, parameters).futures());
      } else {
        throw new IllegalArgumentException(
            String.format("There are %d steps in the session. Cannot add step %d.", userTaskInfo.futures().size(), step));
      }
    } else {
      ensureHeaderNotPresent(httpServletRequest, USER_TASK_HEADER_NAME);
      if (step != 0) {
        throw new IllegalArgumentException(
            String.format("There are no step in the session. Cannot add step %d.", step));
      }
      userTaskId = _uuidGenerator.randomUUID();
      userTaskInfo = insertFuturesByUserTaskId(userTaskId, function, httpServletRequest, parameters);
      // Only create user task id to session mapping for async request
      if (isAsyncRequest) {
        createSessionKeyMapping(userTaskId, httpServletRequest);
      }

      httpServletResponse.setHeader(USER_TASK_HEADER_NAME, userTaskId.toString());
      return Collections.unmodifiableList(userTaskInfo.futures());
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
    Iterator<Map.Entry<UUID, UserTaskInfo>> iter = _uuidToActiveUserTaskInfoMap.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<UUID, UserTaskInfo> entry = iter.next();
      if (entry.getValue().isUserTaskDoneExceptionally()) {
        LOG.warn("UserTask {} is completed with Exception and removed from active tasks list", entry.getKey());
        _uuidToCompletedUserTaskInfoMap.get(entry.getValue().endPoint().endpointType())
                                       .put(entry.getKey(), entry.getValue().setState(TaskState.COMPLETED_WITH_ERROR));
        iter.remove();
        _userTaskLoggerExecutor.submit(() -> entry.getValue().logOperation());
      } else if (entry.getValue().isUserTaskDone()) {
        LOG.info("UserTask {} is completed and removed from active tasks list", entry.getKey());
        _successfulRequestExecutionTimer.get(entry.getValue().endPoint()).update(entry.getValue().executionTimeNs(), TimeUnit.NANOSECONDS);
        _uuidToCompletedUserTaskInfoMap.get(entry.getValue().endPoint().endpointType())
                                       .put(entry.getKey(), entry.getValue().setState(TaskState.COMPLETED));
        iter.remove();
        _userTaskLoggerExecutor.submit(() -> entry.getValue().logOperation());
      }
    }
  }

  private synchronized void removeFromPurgatory(UserTaskInfo userTaskInfo) {
    // Purgatory is null if the two-step verification is disabled.
    if (_purgatory != null) {
      String parameterString = ParameterUtils.caseSensitiveParameterName(userTaskInfo.queryParams(), REVIEW_ID_PARAM);
      if (parameterString != null) {
        int reviewId = Integer.parseInt(userTaskInfo.queryParams().get(parameterString)[0]);
        // Remove submitted request from purgatory.
        try {
          _purgatory.removeSubmitted(reviewId);
          LOG.info("Successfully removed submitted request corresponding to review id {} from purgatory.", reviewId);
        } catch (IllegalStateException ise) {
          LOG.error("Should never attempt to remove this request from purgatory.", ise);
        }
      }
    }
  }

  private synchronized void removeOldUserTasks() {
    LOG.debug("Remove old user tasks");
    for (Map.Entry<EndpointType, Long> retentionByType : _completedUserTaskRetentionTimeMs.entrySet()) {
      long completedUserTaskRetentionTimeMs = retentionByType.getValue();
      for (Iterator<Map.Entry<UUID, UserTaskInfo>> iterator =
           _uuidToCompletedUserTaskInfoMap.get(retentionByType.getKey()).entrySet().iterator(); iterator.hasNext(); ) {
        Map.Entry<UUID, UserTaskInfo> entry = iterator.next();
        if (entry.getValue().startMs() + completedUserTaskRetentionTimeMs < _time.milliseconds()) {
          removeFromPurgatory(entry.getValue());
          iterator.remove();
        }
      }
    }
  }

  /**
   * Update status for user task which is currently being executed.
   *
   * @param uuid UUID associated with the in-execution user task.
   * @return {@link UserTaskInfo} associated with the task in-execution.
   */
  public synchronized UserTaskInfo markTaskExecutionBegan(String uuid) {
    UUID userTaskId = UUID.fromString(uuid);
    for (Map<UUID, UserTaskInfo> infoMap : _uuidToCompletedUserTaskInfoMap.values()) {
      if (infoMap.containsKey(userTaskId)) {
        _inExecutionUserTaskInfo = infoMap.remove(userTaskId).setState(TaskState.IN_EXECUTION);
        return _inExecutionUserTaskInfo;
      }
    }

    if (_uuidToActiveUserTaskInfoMap.containsKey(userTaskId)) {
      _inExecutionUserTaskInfo = _uuidToActiveUserTaskInfoMap.remove(userTaskId).setState(TaskState.IN_EXECUTION);
      // Normally a user task's operation result is logged when the task's state is transferred from ACTIVE to COMPLETED_WITH_ERROR.
      // If the user task's state is transferred from ACTIVE directly to IN_EXECUTION, need to log the task's operation result here.
      _inExecutionUserTaskInfo.logOperation();
    }

    return _inExecutionUserTaskInfo;
  }

  /**
   * Update user task status once the execution is done.
   *
   * @param uuid UUID associated with the in-execution user task.
   */
  public synchronized void markTaskExecutionFinished(String uuid) {
    if (!_inExecutionUserTaskInfo.userTaskId().equals(UUID.fromString(uuid))) {
      throw new IllegalStateException(String.format("Task %s is not found in UserTaskManager.", uuid));
    }
    _inExecutionUserTaskInfo.setState(TaskState.COMPLETED);
    _uuidToCompletedUserTaskInfoMap.get(_inExecutionUserTaskInfo.endPoint().endpointType())
                                   .put(_inExecutionUserTaskInfo.userTaskId(), _inExecutionUserTaskInfo);
    _inExecutionUserTaskInfo = null;
  }

  public synchronized UserTaskInfo getUserTaskByUserTaskId(UUID userTaskId, HttpServletRequest httpServletRequest) {
    if (userTaskId == null) {
      return null;
    }

    String requestUrl = httpServletRequestToString(httpServletRequest);
    for (Map<UUID, UserTaskInfo> infoMap : _uuidToCompletedUserTaskInfoMap.values()) {
      if (infoMap.containsKey(userTaskId)) {
        UserTaskInfo userTaskInfo = infoMap.get(userTaskId);
        if (userTaskInfo.requestUrl().equals(requestUrl)
            && hasTheSameHttpParameter(userTaskInfo.queryParams(), httpServletRequest.getParameterMap())) {
          return userTaskInfo;
        }
      }
    }

    if (_uuidToActiveUserTaskInfoMap.containsKey(userTaskId)) {
      UserTaskInfo userTaskInfo = _uuidToActiveUserTaskInfoMap.get(userTaskId);
      if (userTaskInfo.requestUrl().equals(requestUrl)
          && hasTheSameHttpParameter(userTaskInfo.queryParams(), httpServletRequest.getParameterMap())) {
        return userTaskInfo;
      }
    }

    if (_inExecutionUserTaskInfo != null
        && _inExecutionUserTaskInfo.userTaskId().equals(userTaskId)
        && _inExecutionUserTaskInfo.requestUrl().equals(requestUrl)
        && hasTheSameHttpParameter(_inExecutionUserTaskInfo.queryParams(), httpServletRequest.getParameterMap())) {
      return _inExecutionUserTaskInfo;
    }

    return null;
  }

  /**
   * Common function for adding sync/async tasks
   * @param userTaskId UUID to uniquely identify task.
   * @param operation lambda function to provide result to UserTaskInfo object
   * @param httpServletRequest http request associated with the task
   * @param parameters Parsed parameters from http request, or null if parsing result is unavailable.
   * @return {@link UserTaskInfo} containing request detail and  {@link OperationFuture}
   */
  private synchronized UserTaskInfo insertFuturesByUserTaskId(UUID userTaskId,
                                                              Function<String, OperationFuture> operation,
                                                              HttpServletRequest httpServletRequest,
                                                              CruiseControlParameters parameters) {
    if (_uuidToActiveUserTaskInfoMap.containsKey(userTaskId)) {
      _uuidToActiveUserTaskInfoMap.get(userTaskId).futures().add(operation.apply(userTaskId.toString()));
    } else {
      if (_uuidToActiveUserTaskInfoMap.size() >= _maxActiveUserTasks) {
        throw new RuntimeException("There are already " + _uuidToActiveUserTaskInfoMap.size() +
            " active user tasks, which has reached the servlet capacity.");
      }
      UserTaskInfo userTaskInfo =
          new UserTaskInfo(httpServletRequest, new ArrayList<>(Collections.singleton(operation.apply(userTaskId.toString()))),
                           _time.milliseconds(), userTaskId, TaskState.ACTIVE, parameters);
      _uuidToActiveUserTaskInfoMap.put(userTaskId, userTaskInfo);
    }
    return _uuidToActiveUserTaskInfoMap.get(userTaskId);
  }

  public synchronized List<UserTaskInfo> getAllUserTasks() {
    List<UserTaskInfo> allUserTasks = new ArrayList<>(_uuidToActiveUserTaskInfoMap.values());
    if (_inExecutionUserTaskInfo != null) {
      allUserTasks.add(_inExecutionUserTaskInfo);
    }
    allUserTasks.addAll(_uuidToCompletedUserTaskInfoMap.values().stream().flatMap(map -> map.values().stream())
                                                       .collect(Collectors.toList()));
    return allUserTasks;
  }

  @Override
  public String toString() {
    Map<UUID, UserTaskInfo> uuidToCompletedWithSuccessUserTaskInfoMap = new LinkedHashMap<>();
    Map<UUID, UserTaskInfo> uuidToCompletedWithErrorUserTaskInfoMap = new LinkedHashMap<>();
    for (Map<UUID, UserTaskInfo> infoMap : _uuidToCompletedUserTaskInfoMap.values()) {
      infoMap.forEach((k, v) -> {
        if (v.state() == TaskState.COMPLETED) {
          uuidToCompletedWithSuccessUserTaskInfoMap.put(k, v);
        } else {
          uuidToCompletedWithErrorUserTaskInfoMap.put(k, v);
        }
      });
    }
    return "UserTaskManager{_sessionKeyToUserTaskIdMap=" + _sessionKeyToUserTaskIdMap
           + ", _uuidToActiveUserTaskInfoMap=" + _uuidToActiveUserTaskInfoMap
           + ", _inExecutionUserTask=" + (_inExecutionUserTaskInfo != null ? _inExecutionUserTaskInfo : "No-User-Initiated-Execution")
           + ", _uuidToCompletedWithSuccessUserTaskInfoMap=" + uuidToCompletedWithSuccessUserTaskInfoMap
           + ", _uuidToCompletedWithErrorUserTaskInfoMap=" + uuidToCompletedWithErrorUserTaskInfoMap + '}';
  }

  @Override
  public void close() {
    _userTaskScannerExecutor.shutdownNow();
    _userTaskLoggerExecutor.shutdownNow();
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
  public static class UserTaskInfo {
    private final List<OperationFuture> _futures;
    private final String _requestUrl;
    private final String _clientIdentity;
    private final long _startMs;
    private final UUID _userTaskId;
    private final Map<String, String[]> _queryParams;
    private final EndPoint _endPoint;
    private TaskState _state;
    private final CruiseControlParameters _parameters;

    public UserTaskInfo(HttpServletRequest httpServletRequest,
                        List<OperationFuture> futures,
                        long startMs,
                        UUID userTaskId,
                        TaskState state,
                        CruiseControlParameters parameters) {
      if (futures == null || futures.isEmpty()) {
        throw new IllegalArgumentException("Invalid OperationFuture list " + futures + " is provided for UserTaskInfo.");
      }
      _futures = futures;
      _requestUrl = httpServletRequestToString(httpServletRequest);
      _clientIdentity = KafkaCruiseControlServletUtils.getClientIpAddress(httpServletRequest);
      _startMs = startMs;
      _userTaskId = userTaskId;
      _queryParams = httpServletRequest.getParameterMap();
      _endPoint = ParameterUtils.endPoint(httpServletRequest);
      _state = state;
      _parameters = parameters;
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
      return lastFuture().finishTimeNs() - TimeUnit.MILLISECONDS.toNanos(_startMs);
    }

    private OperationFuture lastFuture() {
      return _futures.get(_futures.size() - 1);
    }

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

    public CruiseControlParameters parameters() {
      return _parameters;
    }

    boolean isUserTaskDone() {
      return lastFuture().isDone();
    }

    boolean isUserTaskDoneExceptionally() {
      return lastFuture().isCompletedExceptionally();
    }

    private void logOperation() {
      try {
        CruiseControlResponse response = lastFuture().get();
        response.discardIrrelevantResponse(_parameters);
        OPERATION_LOG.info("Task [{}] calculation finishes, result:\n{}", _userTaskId, response.cachedResponse());
      } catch (InterruptedException | ExecutionException e) {
        OPERATION_LOG.info("Task [{}] calculation fails, exception:\n{}", _userTaskId, e);
      }
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
   * Possible state of tasks, with supported transitions:
   * <ul>
   *   <li>{@link TaskState#ACTIVE} -&gt; {@link TaskState#IN_EXECUTION}, {@link TaskState#COMPLETED},
   *   {@link TaskState#COMPLETED_WITH_ERROR}</li>
   *   <li>{@link TaskState#COMPLETED} -&gt; {@link TaskState#IN_EXECUTION}(due to thread synchronization issue)</li>
   *   <li>{@link TaskState#IN_EXECUTION} -&gt; {@link TaskState#COMPLETED}</li>
   * </ul>
   */
  public enum TaskState {
    ACTIVE("Active"),
    IN_EXECUTION("InExecution"),
    COMPLETED("Completed"),
    COMPLETED_WITH_ERROR("CompletedWithError");

    private String _type;
    TaskState(String type) {
      _type = type;
    }

    @Override
    public String toString() {
      return _type;
    }

    private static final List<TaskState> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

    /**
     * Use this instead of values() because values() creates a new array each time.
     * @return enumerated values in the same order as values()
     */
    public static List<TaskState> cachedValues() {
      return CACHED_VALUES;
    }
  }
}
