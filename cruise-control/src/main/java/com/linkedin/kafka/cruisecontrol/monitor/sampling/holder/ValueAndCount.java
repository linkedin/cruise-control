/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;


/**
 * A class to give average of the recorded values.
 */
class ValueAndCount implements ValueHolder {
  protected static final double NO_RECORD_EXISTS = -1.0;
  private double _value = 0.0;
  private int _count = 0;

  @Override
  public void recordValue(double value, long time) {
    _value += value;
    _count++;
  }

  @Override
  public void reset() {
    _value = 0.0;
    _count = 0;
  }

  @Override
  public double value() {
    return _count == 0 ? 0.0 : _value / _count;
  }

  /**
   * Assumes that the average of recorded values cannot be {@link #NO_RECORD_EXISTS} when assertNonZeroCount is true.
   */
  @Override
  public double value(boolean assertNonZeroCount) {
    return _count == 0 ? (assertNonZeroCount ? NO_RECORD_EXISTS : 0.0) : _value / _count;
  }
}
