/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeIncreasePartitionCount;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED_WITH_ERROR;


/**
 * A util class for provisions.
 */
public final class ProvisionerUtils {

  private ProvisionerUtils() {
  }

  /**
   * Increase the partition count of the given existing topics to the desired partition count (if needed).
   *
   * @param adminClient AdminClient to handle partition count increases.
   * @param topicsToAddPartitions Existing topics to add more partitions to (if needed). More partitions are added to a topic if it has fewer
   *                              than the number of partitions specified in its corresponding {@link NewTopic#numPartitions()}.
   * @return {@link ProvisionerState.State#COMPLETED} when none of the partition count increase attempts fail,
   * {@link ProvisionerState.State#COMPLETED_WITH_ERROR} otherwise.
   */
  public static ProvisionerState increasePartitionCount(AdminClient adminClient, Set<NewTopic> topicsToAddPartitions) {
    Map<String, Integer> numPartitionsBySucceededTopic = new HashMap<>(topicsToAddPartitions.size());
    Map<String, Integer> numPartitionsByFailedTopic = new HashMap<>(topicsToAddPartitions.size());

    for (NewTopic topicToAddPartitions : topicsToAddPartitions) {
      boolean success = maybeIncreasePartitionCount(adminClient, topicToAddPartitions);
      if (success) {
        numPartitionsBySucceededTopic.put(topicToAddPartitions.name(), topicToAddPartitions.numPartitions());
      } else {
        numPartitionsByFailedTopic.put(topicToAddPartitions.name(), topicToAddPartitions.numPartitions());
      }
    }
    String summary = String.format("Setting partition count by topic succeeded: %s failed: %s.",
                                   numPartitionsBySucceededTopic, numPartitionsByFailedTopic);
    return new ProvisionerState(numPartitionsByFailedTopic.isEmpty() ? COMPLETED : COMPLETED_WITH_ERROR, summary);
  }

  /**
   * Create a new subset map of the given recommendation-by-recommender map containing only the entries, where the resource is a partition.
   * If the given map has no recommendations, where the resource is a partition, then the response would be an empty map.
   *
   * @param recommendationByRecommender Provision recommendations provided by corresponding recommenders.
   * @return A new subset map of the given recommendation-by-recommender map containing only the entries, where the resource is a partition.
   */
  public static Map<String, ProvisionRecommendation> partitionRecommendations(Map<String, ProvisionRecommendation> recommendationByRecommender) {
    Map<String, ProvisionRecommendation> partitionRecommendations = new HashMap<>();
    for (Map.Entry<String, ProvisionRecommendation> recommendationEntry : recommendationByRecommender.entrySet()) {
      if (recommendationEntry.getValue().numPartitions() != ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
        partitionRecommendations.put(recommendationEntry.getKey(), recommendationEntry.getValue());
      }
    }
    return partitionRecommendations;
  }
}
