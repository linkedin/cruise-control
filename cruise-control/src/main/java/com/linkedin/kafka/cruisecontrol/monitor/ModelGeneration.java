/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import java.io.Serializable;


/**
 * A container class to hold generation information.
 */
public class ModelGeneration implements Serializable {
  private static final long serialVersionUID = 1494955635123L;

  private final int _clusterGeneration;
  private final long _loadGeneration;
  private int hash = 0;

  public ModelGeneration(int clusterGeneration, long loadGeneration) {
    _clusterGeneration = clusterGeneration;
    _loadGeneration = loadGeneration;
  }

  public int clusterGeneration() {
    return _clusterGeneration;
  }

  public long loadGeneration() {
    return _loadGeneration;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof ModelGeneration)) {
      return false;
    }

    ModelGeneration other = (ModelGeneration) o;
    return _clusterGeneration == other.clusterGeneration() && _loadGeneration == other.loadGeneration();
  }

  @Override
  public int hashCode() {
    if (hash != 0) {
      return hash;
    }
    final int prime = 31;
    int result = 1;
    result = prime * result + _clusterGeneration;
    result = prime * result + (int) _loadGeneration;
    this.hash = result;
    return result;
  }

  @Override
  public String toString() {
    return String.format("[ClusterGeneration=%d,LoadGeneration=%d]", _clusterGeneration, _loadGeneration);
  }
}
