/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

/**
 * An interface to unify the {@link ValueAndTime}, {@link ValueAndCount}, and {@link ValueMax}. The meaning of recording
 * a value differs depending on the custom implementation of this interface.
 */
interface ValueHolder {

  /**
   * @param value The value to record.
   * @param time The time to record (if relevant)
   */
  void recordValue(double value, long time);

  /**
   * Reset the value holder history to the clean state after creation.
   */
  void reset();

  /**
   * @return The value associated with the holder.
   */
  double value();

  /**
   * @param assertNonZeroCount {@code true} to assert that at least a single record exist, {@code false} otherwise.
   * @return The value associated with the holder, or custom value if no record exists when non zero count is asserted.
   */
  double value(boolean assertNonZeroCount);
}
