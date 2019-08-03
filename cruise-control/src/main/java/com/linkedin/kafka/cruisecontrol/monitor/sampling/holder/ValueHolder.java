/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

/**
 * An interface to unify the {@link ValueAndTime} and {@link ValueAndCount}
 */
interface ValueHolder {
  void recordValue(double value, long time);
  void reset();
  double value();
  double value(boolean assertNonZeroCount);
}
