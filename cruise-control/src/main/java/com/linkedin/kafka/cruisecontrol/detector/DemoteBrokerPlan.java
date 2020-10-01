/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Set;


public class DemoteBrokerPlan extends MaintenancePlanWithBrokers {
  public static final byte LATEST_SUPPORTED_VERSION = 0;

  public DemoteBrokerPlan(long timeMs, int brokerId, Set<Integer> brokers) {
    super(MaintenanceEventType.DEMOTE_BROKER, timeMs, brokerId, LATEST_SUPPORTED_VERSION, brokers);
  }
}
