/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common;

/**
 * An interface that helps maintain and compare the generation.
 */
public interface Generationed<G> {

  /**
   * Set the generation.
   * @param generation the new generation.
   */
  void setGeneration(G generation);

  /**
   * Get the current generation.
   * @return The current generation.
   */
  G generation();

  /**
   * Compare the generation of this object with the other object.
   * We are not extending {@link Comparable} but define our own method to avoid the case that users wants to
   * have a different comparing method along with the generation comparison.
   *
   * @param other another generationed object to compare the generation with.
   * @return -1 if the the generation of this object is earlier than the other's. 0 when the generations are the same.
   * 1 when the generation of this object is later than the other's.
   */
  int compareGeneration(Generationed<G> other);

  /**
   * Compare the generation of this object with the given generation.
   *
   * @param generation the given generation.
   * @return -1 if the the generation of this object is earlier than the given generation.
   * 0 when the generations are the same.
   * 1 when the generation of this object is later than the given one.
   */
  int compareGeneration(G generation);
}
