/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model;

/**
 * The abstract class for an entity. We use abstract class to force implementation of {@link #hashCode()} and
 * {@link #equals(Object)} method.
 *
 * @param <G> the group this entity belongs to.
 */
public abstract class Entity<G> {

  /**
   * Note that the group will be used as keys of maps. So it should implement equals() and hashCode() if necessary.
   * @return The entity group of this entity
   */
  public abstract G group();

  /**
   * The entity will be used as a key of a map. So it should implement hashCode() and equals().
   * {@inheritDoc}
   */
  @Override
  public abstract int hashCode();

  /**
   * The entity will be used as a key of a map. So it should implement hashCode() and equals().
   * {@inheritDoc}
   */
  @Override
  public abstract boolean equals(Object other);
}
