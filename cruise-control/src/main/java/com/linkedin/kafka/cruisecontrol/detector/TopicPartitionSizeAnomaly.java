/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.detector.TopicPartitionSizeAnomalyFinder.PARTITIONS_WITH_LARGE_SIZE_CONFIG;


/**
 * Topic partitions with size larger than
 * {@link com.linkedin.kafka.cruisecontrol.detector.TopicPartitionSizeAnomalyFinder#SELF_HEALING_PARTITION_SIZE_THRESHOLD_CONFIG}
 */
public class TopicPartitionSizeAnomaly extends TopicAnomaly {
  protected Map<TopicPartition, Double> _sizeByPartition;

  /**
   * There are two potential ways to fix partitions with large size, i.e. increasing topic's partition count and reducing
   * topic's retention time/size.
   * But both ways could break the client-side applications, therefore it is safer for Cruise Control to just send out alert
   * and not try to self-heal the anomaly.
   *
   * @return True if fix was started successfully (i.e. there is actual work towards a fix), false otherwise.
   */
  @Override
  public boolean fix() {
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _sizeByPartition = (Map<TopicPartition, Double>) configs.get(PARTITIONS_WITH_LARGE_SIZE_CONFIG);
    if (_sizeByPartition == null || _sizeByPartition.isEmpty()) {
      throw new IllegalArgumentException(String.format("Missing %s for topic partition size anomaly.", PARTITIONS_WITH_LARGE_SIZE_CONFIG));
    }
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> "";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{Detected following topic partitions having too large size:\n");
    for (Map.Entry<TopicPartition, Double> entry : _sizeByPartition.entrySet()) {
      sb.append(String.format("\t%s\t%f%n", entry.getKey().toString(), entry.getValue()));
    }
    sb.append("}");
    return sb.toString();
  }
}
