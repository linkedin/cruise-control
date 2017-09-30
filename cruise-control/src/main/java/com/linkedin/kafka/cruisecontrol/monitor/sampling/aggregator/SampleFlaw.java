/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;


/**
 * The sample flaw for a partition that is still treated as valid.
 */
public class SampleFlaw {
  private final long _snapshotWindow;
  private final Extrapolation _imputation;

  public SampleFlaw(long snapshotWindow, Extrapolation imputation) {
    _snapshotWindow = snapshotWindow;
    _imputation = imputation;
  }

  public long snapshotWindow() {
    return _snapshotWindow;
  }

  public Extrapolation imputation() {
    return _imputation;
  }

  @Override
  public String toString() {
    return String.format("[%d, %s]", _snapshotWindow, _imputation);
  }
}
