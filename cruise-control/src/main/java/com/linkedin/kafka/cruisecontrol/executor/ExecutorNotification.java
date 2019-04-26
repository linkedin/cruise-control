/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;

/**
 * A class to encapsulate notification information sent to requester associated with an execution.
 */
public class ExecutorNotification {
  private ActionAgent _startedBy;       // Indicate origin of the execution, could be User or Cruise Control
  private ActionAgent _endedBy;         // An executions can be ended by Cruise Control (due to Exception or completion) or by User
  private final long _startMs;
  private final long _endMs;
  private final UserTaskManager.UserTaskInfo _userTaskInfo;
  private final boolean _executionSucceeded;
  private String _operation;            // Can be UserTask Endpoint or Self-healing action
  private Throwable _exception = null;  // The Exception that ended the execution
  private final String _actionUuid;    // UUID associated with execution, could be for UserTask for Self-healing

  public ExecutorNotification(long executionStartMs,
                              long endMs,
                              UserTaskManager.UserTaskInfo userTaskInfo,
                              String uuid,
                              boolean stopRequested,
                              boolean executionStoppedByUser,
                              Throwable executionException,
                              boolean executionSucceeded) {
    _startMs = executionStartMs;
    _endMs = endMs;
    _userTaskInfo = userTaskInfo;
    _startedBy = ActionAgent.UNKNOWN;
    _operation = "UNKNOWN";
    _actionUuid = uuid;
    _executionSucceeded = executionSucceeded;

    if (uuid != null) {
      if (_userTaskInfo != null) {
        _startedBy = ActionAgent.USER;
        _operation = _userTaskInfo.endPoint().toString();
      } else {
        // UUID with anomaly prefix are not present in link UserTaskManager
        for (AnomalyType type : AnomalyType.cachedValues()) {
          if (_actionUuid.startsWith(type.toString())) {
            _startedBy = ActionAgent.CRUISE_CONTROL;
            _operation = type.toString() + " Fix Action";
            break;
          }
        }
      }
    }

    _endedBy = ActionAgent.EXECUTION_COMPLETION;
    if (stopRequested) {
      if (executionStoppedByUser) {
        _endedBy = ActionAgent.USER;
      } else {
        _endedBy = ActionAgent.CRUISE_CONTROL;
      }
    } else if (executionException != null) {
      _endedBy = ActionAgent.EXCEPTION;
      _exception = executionException;
    }
  }

  public ActionAgent startedBy() {
    return _startedBy;
  }

  public ActionAgent endedBy() {
    return _endedBy;
  }

  public long startMs() {
    return _startMs;
  }

  public long endMs() {
    return _endMs;
  }

  public UserTaskManager.UserTaskInfo userTaskInfo() {
    return _userTaskInfo;
  }

  public boolean executionSucceeded() {
    return _executionSucceeded;
  }

  public String operation() {
    return _operation;
  }

  public String actionUuid() {
    return _actionUuid;
  }

  public boolean startedByUser() {
    return _startedBy == ActionAgent.USER;
  }

  public Throwable exception() {
    return _exception;
  }

  // Possible agent or event to cause Cruise Control execution state change
  public enum ActionAgent {
    USER,
    CRUISE_CONTROL,
    EXCEPTION,
    EXECUTION_COMPLETION,
    UNKNOWN
  }
}
