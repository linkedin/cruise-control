/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

/**
 * The interface to get metric samples of given topic partitions.
 * <p>
 * Kafka Cruise Control periodically collects the metrics of all the partitions in the cluster, including the leader and follower
 * replicas. The {@link #getSamples(Cluster, Set, long, long, SamplingMode, MetricDef, long)}
 * will be called for all the replicas of partitions in the cluster in one sampling period.
 * The MetricSampler may be used by multiple threads at the same time, so the implementation need to be thread safe.
 *
 */
public interface MetricSampler extends CruiseControlConfigurable, AutoCloseable {
  Samples EMPTY_SAMPLES = new Samples(Collections.emptySet(), Collections.emptySet());

  /**
   * Get the metric sample of the given topic partition and replica from the Kafka cluster.
   *
   * The samples include PartitionMetricSamples and BrokerMetricSamples.
   *
   * Due to the lack of direct metrics at partition level, Kafka Cruise Control needs to estimate the CPU
   * utilization for each partition by using the following formula:
   *
   *  BROKER_CPU_UTIL = a * ALL_TOPIC_BYTES_IN_RATE + b * ALL_TOPIC_BYTES_OUT_RATE + c * ALL_FOLLOWER_BYTES_IN_RATE
   *
   *  LEADER_PARTITION_CPU_UTIL = a * LEADER_PARTITION_BYTES_IN + b * LEADER_PARTITION_BYTES_OUT
   *
   *  FOLLOWER_PARTITION_CPU_UTIL = c * LEADER_PARTITION_BYTES_IN
   *
   * Kafka Cruise Control needs to know the parameters of a, b and c for cost evaluation of leader and
   * partition movement.
   *
   * @param cluster the metadata of the cluster.
   * @param assignedPartitions the topic partition
   * @param startTimeMs the start time of the sampling period.
   * @param endTimeMs the end time of the sampling period.
   * @param mode The sampling mode.
   * @param metricDef the metric definitions.
   * @param timeout The sampling timeout to stop sampling even if there is more data to get.
   * @return the PartitionMetricSample of the topic partition and replica id
   */
  Samples getSamples(Cluster cluster,
                     Set<TopicPartition> assignedPartitions,
                     long startTimeMs,
                     long endTimeMs,
                     SamplingMode mode,
                     MetricDef metricDef,
                     long timeout)
      throws MetricSamplingException;

  /**
   * The sampling mode to indicate which type of samples is interested.
   */
  enum SamplingMode {
    PARTITION_METRICS_ONLY, BROKER_METRICS_ONLY, ALL
  }

  /**
   * A container class that wraps both the partition metric samples and broker metric samples.
   */
  class Samples {
    private final Set<PartitionMetricSample> _partitionMetricSamples;
    private final Set<BrokerMetricSample> _brokerMetricSamples;

    public Samples(Set<PartitionMetricSample> partitionMetricSamples,
                   Set<BrokerMetricSample> brokerMetricSamples) {
      _partitionMetricSamples = partitionMetricSamples;
      _brokerMetricSamples = brokerMetricSamples;
    }

    public Set<PartitionMetricSample> partitionMetricSamples() {
      return _partitionMetricSamples;
    }

    public Set<BrokerMetricSample> brokerMetricSamples() {
      return _brokerMetricSamples;
    }
  }
}
