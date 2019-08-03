/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;


/**
 * A class to give the latest of the recorded values.
 */
class ValueAndCount implements ValueHolder {
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
    return value(false);
  }

  @Override
  public double value(boolean assertNonZeroCount) {
    return _count == 0 ? (assertNonZeroCount ? -1.0 : 0.0) : _value / _count;
  }
}
