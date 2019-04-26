/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * For sending notification about executor completion status.
 */
@InterfaceStability.Evolving
public interface ExecutorNotifier extends CruiseControlConfigurable {
  /**
   * When an execution completes successfully or is stopped this method should be called
   * @param notification Information to be sent.
   */
  void sendNotification(ExecutorNotification notification);
}
