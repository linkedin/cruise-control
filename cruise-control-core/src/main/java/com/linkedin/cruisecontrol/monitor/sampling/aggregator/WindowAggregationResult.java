/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.common.LongGenerationed;
import com.linkedin.cruisecontrol.monitor.sampling.Snapshot;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.FORCED_INSUFFICIENT;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.NONE;
import static com.linkedin.cruisecontrol.monitor.sampling.aggregator.SnapshotAndImputation.Imputation.NO_VALID_IMPUTATION;


/**
 * The metric aggregation result for a snapshot window.
 */
public class WindowAggregationResult<E> extends LongGenerationed {
  private final long _window;
  private final Map<E, Snapshot> _snapshots;
  private final Map<E, SnapshotAndImputation.Imputation> _entitiesWithImputation;
  private final Map<E, SnapshotAndImputation.Imputation> _invalidEntities;

  WindowAggregationResult(long window, long generation) {
    super(generation);
    _window = window;
    _snapshots = new HashMap<>();
    _entitiesWithImputation = new HashMap<>();
    _invalidEntities = new HashMap<>();
  }

  void addSnapshot(E entity, SnapshotAndImputation snapshotAndImputation) {
    Snapshot s = _snapshots.putIfAbsent(entity, snapshotAndImputation.snapshot());
    if (s != null) {
      throw new IllegalStateException("Snapshot for " + entity + " has already exists for window " + _window);
    }
    SnapshotAndImputation.Imputation imputation = snapshotAndImputation.imputation();
    if (imputation != NONE) {
      _entitiesWithImputation.put(entity, imputation);
      if (imputation == FORCED_INSUFFICIENT || imputation == NO_VALID_IMPUTATION) {
        _invalidEntities.put(entity, imputation);
      }
    }
  }

  public long window() {
    return _window;
  }

  public Map<E, Snapshot> snapshots() {
    return Collections.unmodifiableMap(_snapshots);
  }

  public Map<E, SnapshotAndImputation.Imputation> entityWithImputations() {
    return Collections.unmodifiableMap(_entitiesWithImputation);
  }

  public Map<E, SnapshotAndImputation.Imputation> invalidEntities() {
    return Collections.unmodifiableMap(_invalidEntities);
  }

  @Override
  public void setGeneration(Long generation) {
    throw new IllegalStateException("The generation of WindowAggregationResult should be unmodifiable.");
  }
}
