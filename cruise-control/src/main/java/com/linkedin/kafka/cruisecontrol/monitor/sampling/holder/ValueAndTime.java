/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;


/**
 * A class to give average of the recorded values.
 */
class ValueAndTime implements ValueHolder {
  private double _value = 0.0;
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
    _value = 0.0;
    _time = -1;
  }

  @Override
  public double value() {
    return _value;
  }

  @Override
  public double value(boolean assertNonZeroCount) {
    return _value;
  }
}