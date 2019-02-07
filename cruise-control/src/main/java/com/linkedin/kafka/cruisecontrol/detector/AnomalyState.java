/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * A class to indicate how an anomaly is handled.
 */
public class AnomalyState {
  private final Anomaly _anomaly;
  private final long _detectionMs;
  private long _statusUpdateMs;
  private Status _status;

  public AnomalyState(Anomaly anomaly) {
    _anomaly = anomaly;
    _detectionMs = System.currentTimeMillis();
    _statusUpdateMs = _detectionMs;
    _status = Status.DETECTED;
  }

  public Status status() {
    return _status;
  }

  public String anomalyId() {
    return _anomaly.anomalyId();
  }

  public Anomaly anomaly() {
    return _anomaly;
  }

  public long detectionMs() {
    return _detectionMs;
  }

  public long statusUpdateMs() {
    return _statusUpdateMs;
  }

  public void setStatus(Status status) {
    _status = status;
    _statusUpdateMs = System.currentTimeMillis();
  }

  public enum Status {
    DETECTED, IGNORED, FIX_STARTED, FIX_FAILED_TO_START, CHECK_WITH_DELAY, LOAD_MONITOR_NOT_READY, COMPLETENESS_NOT_READY;

    private static final List<Status> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

    /**
     * Use this instead of values() because values() creates a new array each time.
     * @return enumerated values in the same order as values()
     */
    public static List<Status> cachedValues() {
      return CACHED_VALUES;
    }
  }
}
