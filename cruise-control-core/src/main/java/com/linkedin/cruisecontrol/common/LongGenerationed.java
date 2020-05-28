/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common;

import java.util.concurrent.atomic.AtomicLong;


public class LongGenerationed implements Generationed<Long> {
  protected final AtomicLong _generation;

  public LongGenerationed(long generation) {
    _generation = new AtomicLong(generation);
  }

  @Override
  public void setGeneration(Long generation) {
    _generation.set(generation);
  }

  @Override
  public Long generation() {
    return _generation.get();
  }

  /**
   * {@inheritDoc}
   * When {@code other} is null, this LongGenerationed will be considered as a later generation.
   */
  @Override
  public int compareGeneration(Generationed<Long> other) {
    if (other == null) {
      return 1;
    }
    return Long.compare(_generation.get(), other.generation());
  }

  @Override
  public int compareGeneration(Long generation) {
    return Long.compare(_generation.get(), generation);
  }
}
