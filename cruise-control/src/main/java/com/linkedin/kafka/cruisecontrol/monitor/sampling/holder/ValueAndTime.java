/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;


/**
 * A class to give the latest of the recorded values.
 */
class ValueAndTime implements ValueHolder {
  protected static final double NO_RECORD_EXISTS = 0.0;
  private double _value = NO_RECORD_EXISTS;
  private long _time = -1;

  @Override
  public void recordValue(double value, long time) {
    if (time > _time) {
      _value = value;
      _time = time;
    }
  }

  @Override
  public void reset() {
    _value = NO_RECORD_EXISTS;
    _time = -1;
  }

  @Override
  public double value() {
    return _value;
  }

  /**
   * Assumes that the latest recorded value cannot be {@link #NO_RECORD_EXISTS} when assertNonZeroCount is true.
   */
  @Override
  public double value(boolean assertNonZeroCount) {
    return _value;
  }
}
