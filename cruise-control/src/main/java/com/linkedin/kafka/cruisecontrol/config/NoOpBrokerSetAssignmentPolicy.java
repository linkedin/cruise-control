/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * For any non-mapped Brokers, this policy does not assign any broker set.
 * Works as a strict broker set mapping policy where only the assignments that are pre-defined are the true source of truth.
 */
public class NoOpBrokerSetAssignmentPolicy implements BrokerSetAssignmentPolicy {
  private static final String UNMAPPED_BROKER_SET_ID = "unmapped";

  /**
   * Does not assign any broker set to non-mapped brokers.
   *
   * @return A map of broker Ids by their broker set Id
   */
  @Override
  public Map<String, Set<Integer>> assignBrokerSetsForUnresolvedBrokers(final ClusterModel clusterModel,
                                                                        final Map<String, Set<Integer>> existingBrokerSetMapping)
      throws BrokerSetResolutionException {
    // Sanity check to check if all brokers in data store do not match all brokers in cluster model
    Set<Broker> allMappedBrokers = existingBrokerSetMapping.values()
                                                           .stream()
                                                           .flatMap(brokerIds -> brokerIds.stream())
                                                           .map(brokerId -> clusterModel.broker(brokerId))
                                                           .filter(Objects::nonNull)
                                                           .collect(Collectors.toSet());

    Set<Broker> extraBrokersInClusterModel = new HashSet<>(clusterModel.brokers());
    extraBrokersInClusterModel.removeAll(allMappedBrokers);

    boolean extraClusterModelBrokersHaveReplicas = extraBrokersInClusterModel.stream().anyMatch(broker -> !broker.replicas().isEmpty());
    // The broker list in data store may not be atomically updated when brokers are added to the cluster
    // In this case we can ignore the brokers if they have no replicas placed
    if (!allMappedBrokers.equals(clusterModel.brokers()) && extraClusterModelBrokersHaveReplicas) {
      throw new BrokerSetResolutionException(
          String.format("All Brokers from data store %s do not match brokers in cluster model %s.", allMappedBrokers,
                        clusterModel.brokers()));
    }

    Set<Integer> unmappedEmptyBrokerIds = extraBrokersInClusterModel.stream()
                                                                    .filter(broker -> broker.replicas().isEmpty())
                                                                    .map(broker -> broker.id())
                                                                    .collect(Collectors.toSet());

    if (!unmappedEmptyBrokerIds.isEmpty()) {
      existingBrokerSetMapping.computeIfAbsent(UNMAPPED_BROKER_SET_ID, k -> new HashSet<>(unmappedEmptyBrokerIds))
                              .addAll(unmappedEmptyBrokerIds);
    }

    return existingBrokerSetMapping;
  }
}
