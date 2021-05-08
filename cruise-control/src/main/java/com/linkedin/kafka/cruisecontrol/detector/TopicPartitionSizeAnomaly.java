/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.detector.PartitionSizeAnomalyFinder.PARTITIONS_WITH_LARGE_SIZE_CONFIG;


/**
 * Topic partitions with size larger than
 * {@link com.linkedin.kafka.cruisecontrol.detector.PartitionSizeAnomalyFinder#SELF_HEALING_PARTITION_SIZE_THRESHOLD_BYTE_CONFIG}
 *
 * Note this class does not try to self-heal partitions with large size, because all the potential fixing operations have the
 * risk of breaking the client-side applications. For example, adding more partitions to the topic can make each partition handle
 * less data, but if the topic's consumer group explicitly assign partition to consumer then the newly added partitions will
 * have no consumer to consume.
 */
public class TopicPartitionSizeAnomaly extends TopicAnomaly {
  protected Map<TopicPartition, Double> _sizeByPartition;

  /**
   * @return An unmodifiable version of the actual bad topic partitions size
   */
  public Map<TopicPartition, Double> getSizeByPartition() {
    return Collections.unmodifiableMap(_sizeByPartition);
  }

  /**
   * Fix the anomaly.
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
    return () -> String.format("Self healing for topic partition size anomaly: %s", this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{Detected following topic partitions having too large size: ");
    for (Map.Entry<TopicPartition, Double> entry : _sizeByPartition.entrySet()) {
      sb.append(String.format("%s : %f bytes, ", entry.getKey().toString(), entry.getValue()));
    }
    sb.setLength(sb.length() - 2);
    sb.append("}");
    return sb.toString();
  }
}
