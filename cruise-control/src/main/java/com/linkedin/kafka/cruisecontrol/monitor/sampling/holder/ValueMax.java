/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import static java.lang.Math.max;


/**
 * A class to give the maximum of the recorded values.
 */
class ValueMax implements ValueHolder {
  protected static final double NO_RECORD_EXISTS = -1.0;
  private double _value = NO_RECORD_EXISTS;

  @Override
  public void recordValue(double value, long time) {
    _value = max(value, _value);
  }

  @Override
  public void reset() {
    _value = NO_RECORD_EXISTS;
  }

  @Override
  public double value() {
    return _value;
  }

  /**
   * Assumes that the maximum recorded value cannot be {@link #NO_RECORD_EXISTS} when assertNonZeroCount is true.
   */
  @Override
  public double value(boolean assertNonZeroCount) {
    return _value;
  }
}
