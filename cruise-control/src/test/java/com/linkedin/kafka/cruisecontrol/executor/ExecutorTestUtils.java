/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.concurrent.TimeUnit;


public final class ExecutorTestUtils {

  static final String RANDOM_UUID = "random_uuid";
  // A UUID to test the proposal execution to be started with UNKNOWN_UUID, but the executor received RANDOM_UUID.
  static final String UNKNOWN_UUID = "unknown_uuid";
  static final Integer BROKER_ID_PLACEHOLDER = 0;
  static final long EXECUTION_ID_PLACEHOLDER = 0;
  static final long REMOVAL_HISTORY_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(12);
  static final long DEMOTION_HISTORY_RETENTION_TIME_MS = TimeUnit.DAYS.toMillis(1);
  static final long PRODUCE_SIZE_IN_BYTES = 10000L;
  static final long EXECUTION_DEADLINE_MS = TimeUnit.SECONDS.toMillis(30);
  static final long EXECUTION_SHORT_CHECK_MS = 10L;
  static final long EXECUTION_REGULAR_CHECK_MS = 100L;
  static final long LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS = 1000L;
  static final int LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS = 1;
  static final long EXECUTION_ALERTING_THRESHOLD_MS = 100L;

  private ExecutorTestUtils() {
  }
}
