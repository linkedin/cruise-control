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
  private long _startMs;
  private long _endMs;
  private UserTaskManager.UserTaskInfo _userTaskInfo;
  private boolean _executionSucceeded;
  private String _operation;            // Can be UserTask Endpoint or Self-healing action
  private Throwable _exception = null;  // The Exception that ended the execution
  private String _actionUuid = null;    // UUID associated with execution, could be for UserTask for Self-healing

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

    if (uuid != null) {
      _actionUuid = uuid;
      if (_userTaskInfo != null) {
        // UUID with anomaly prefix are not present in {@link UserTaskManager}
        _startedBy = ActionAgent.USER;
        _operation = _userTaskInfo.endPoint().toString();
      } else {
        for (AnomalyType type : AnomalyType.cachedValues()) {
          if (_actionUuid.startsWith(type.toString())) {
            _startedBy = ActionAgent.CRUISE_CONTROL;
            _operation = type.toString() + " Fix Action";
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
    _executionSucceeded = executionSucceeded;
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
