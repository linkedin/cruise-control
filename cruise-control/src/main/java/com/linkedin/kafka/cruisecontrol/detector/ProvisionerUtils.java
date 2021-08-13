/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeIncreasePartitionCount;

/**
 * A util class for provisions.
 */
public final class ProvisionerUtils {

  private ProvisionerUtils() {
  }

  /**
   * Determine the status of increasing the partition count
   *
   * @param adminClient AdminClient to handle partition count increases.
   * @param topicToAddPartitions Existing topic to add more partitions if needed
   * @return The state {@link ProvisionerState.State#COMPLETED} when true, {@link ProvisionerState.State#COMPLETED_WITH_ERROR} when false
   */
  public static ProvisionerState partitionIncreaseStatus(AdminClient adminClient, NewTopic topicToAddPartitions) {
    if (maybeIncreasePartitionCount(adminClient, topicToAddPartitions)) {
      String summary = String.format("Provisioning the partition count to increase to %d for the topic %s was successfully completed.",
                                     topicToAddPartitions.numPartitions(),
                                     topicToAddPartitions.name());
      return new ProvisionerState(ProvisionerState.State.COMPLETED, summary);
    } else {
      String summary = String.format("Provisioning the partition count to increase to %d for the topic %s was rejected.",
                                     topicToAddPartitions.numPartitions(),
                                     topicToAddPartitions.name());
      return new ProvisionerState(ProvisionerState.State.COMPLETED_WITH_ERROR, summary);
    }
  }
}
