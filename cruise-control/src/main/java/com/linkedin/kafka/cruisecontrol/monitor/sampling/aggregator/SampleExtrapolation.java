/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;


/**
 * The sample with extrapolation for a partition that is still treated as valid.
 */
public class SampleExtrapolation {
  private final long _window;
  private final Extrapolation _extrapolation;

  public SampleExtrapolation(long window, Extrapolation extrapolation) {
    _window = window;
    _extrapolation = extrapolation;
  }

  /**
   * @return The window of this sample flaw.
   */
  public long window() {
    return _window;
  }

  /**
   * @return The detail {@link Extrapolation} of this flaw.
   */
  public Extrapolation extrapolation() {
    return _extrapolation;
  }

  @Override
  public String toString() {
    return String.format("[%d, %s]", _window, _extrapolation);
  }
}
