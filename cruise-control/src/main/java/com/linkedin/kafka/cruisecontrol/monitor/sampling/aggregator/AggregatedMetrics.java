/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;


/**
 * The class that help aggregate the metric samples by different resource types. This class also maintains
 * the number of samples aggregated for validity check.
 */
class AggregatedMetrics {
  private int _numSamples;
  private float[] _metrics;

  AggregatedMetrics() {
    _numSamples = 0;
    _metrics = new float[Resource.values().length];
  }

  synchronized AggregatedMetrics addSample(PartitionMetricSample sample) {
    for (Resource resource : Resource.values()) {
      if (resource == Resource.DISK) {
        _metrics[resource.id()] = sample.metricFor(Resource.DISK).floatValue();
      } else {
        _metrics[resource.id()] += sample.metricFor(resource);
      }
    }
    _numSamples++;
    return this;
  }

  synchronized int numSamples() {
    return _numSamples;
  }

  /**
   * Check whether we have enough samples for the snapshot.
   */
  synchronized boolean enoughSamples(int minSamplesPerSnapshot) {
    return _numSamples >= minSamplesPerSnapshot;
  }

  /**
   * Convert the aggregated metrics to a {@link Snapshot}.
   * The way we do the conversion is to take an average of CPU, network inbound and outbound traffic. For disk
   * we use the last value.
   *
   * Notice that we are not dealing with potential replication factor change here. In order to deal with that,
   * we need to change the PartitionMetricSample to also record the replication bytes. The replication bytes can only
   * be derived from the replication factor by assuming each replica is up to date. This can be done in a follow-up
   * patch.
   * TODO: Deal with replication factor change for network outbound.
   */
  synchronized Snapshot toSnapshot(long snapshotWindow) {
    return new Snapshot(snapshotWindow,
                        _metrics[Resource.CPU.id()] / _numSamples,
                        _metrics[Resource.NW_IN.id()] / _numSamples,
                        _metrics[Resource.NW_OUT.id()] / _numSamples,
                        _metrics[Resource.DISK.id()]);
  }
}
