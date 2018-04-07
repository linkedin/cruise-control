/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;

/**
 * The interface to assign the partitions to the metric samplers.
 */
public interface MetricSamplerPartitionAssignor extends CruiseControlConfigurable {

  /**
   * Assign the partitions in the cluster to the metric fetchers.
   *
   * @param cluster        The Kafka cluster.
   * @param numFetchers    The number of metric fetchers.
   * @return A List of partition assignment for each of the fetchers.
   */
  List<Set<TopicPartition>> assignPartitions(Cluster cluster, int numFetchers);

}
