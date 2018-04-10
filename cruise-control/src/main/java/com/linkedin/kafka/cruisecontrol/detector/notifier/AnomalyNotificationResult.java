/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

/**
 * The result of an anomaly notification.
 */
public class AnomalyNotificationResult {
  public enum Action {
    IGNORE, FIX, CHECK
  }

  private final Action _action;
  private final long _delay;

  public static AnomalyNotificationResult ignore() {
    return new AnomalyNotificationResult(Action.IGNORE, -1L);
  }

  public static AnomalyNotificationResult fix() {
    return new AnomalyNotificationResult(Action.FIX, -1L);
  }

  public static AnomalyNotificationResult check(long delay) {
    return new AnomalyNotificationResult(Action.CHECK, delay);
  }

  private AnomalyNotificationResult(Action action, long delay) {
    if (action == Action.IGNORE && delay > 0) {
      throw new IllegalArgumentException("The ignore action should not have a delay.");
    }
    _action = action;
    _delay = delay;
  }

  public Action action() {
    return _action;
  }

  public long delay() {
    return _delay;
  }

  @Override
  public String toString() {
    return "{" + _action + "," + _delay + "}";
  }
}
