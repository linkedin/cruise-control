/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import org.apache.kafka.common.Configurable;


/**
 * This interface is for users to implement a sample store which persists the samples stored in Kafka Cruise Control.
 * The sample store will be used by Kafka Cruise Control when it bootstraps.
 */
public interface SampleStore extends Configurable {
  /**
   * Store all the samples to the sample store.
   * @param samples the samples to store.
   */
  void storeSamples(MetricSampler.Samples samples);

  /**
   * Load the samples from the sample store.
   *
   * @param sampleLoader the sample loader that takes in samples.
   */
  void loadSamples(SampleLoader sampleLoader);

  /**
   * Get the sample loading progress. The return value should be between 0 and 1.
   */
  double sampleLoadingProgress();

  /**
   * This method is called when a workload snapshot window is evicted. The snapshot window timestamp will be
   * passed to the method.
   *
   * @param timestamp the timestamp of the snapshot window that has just been evicted.
   */
  void evictSamplesBefore(long timestamp);

  /**
   * Close the sample store.
   */
  void close();

  /**
   * A class that will be constructed by Kafka Cruise Control and used by sample store during sample loading time.
   * This class is to simplify user interface.
   */
  class SampleLoader {
    private final MetricSampleAggregator _metricSampleAggregator;

    public SampleLoader(MetricSampleAggregator metricSampleAggregator) {
      _metricSampleAggregator = metricSampleAggregator;
    }

    public void loadSamples(MetricSampler.Samples samples) {
      for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
        _metricSampleAggregator.addSample(sample, false, false);
      }
      ModelParameters.addMetricObservation(samples.brokerMetricSamples());
    }
  }
}
