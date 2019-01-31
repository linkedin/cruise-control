/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.executor.Executor;
import java.util.Map;

/**
 * A no-op notifier for executor.
 */
public class ExecutorNoopNotifier implements ExecutorNotifier {
  @Override
  public void sendNotification(Executor.ExecutionNotifierPayload payload) { }
  @Override
  public void configure(Map<String, ?> configs) { }
}
