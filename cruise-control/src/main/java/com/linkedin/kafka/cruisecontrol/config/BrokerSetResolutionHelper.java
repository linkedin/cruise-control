/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;


/**
 * This is a helper class to contain helper functions to get brokerSet mappings and reverse mappings
 */
public class BrokerSetResolutionHelper {
  private Map<String, Set<Broker>> _brokersByBrokerSetId;
  private Map<Integer, String> _brokerSetIdByBrokerId;

  public BrokerSetResolutionHelper(ClusterModel clusterModel, BrokerSetResolver brokerSetResolver) throws BrokerSetResolutionException {
    initializeBrokerSetMappings(clusterModel, brokerSetResolver);
  }

  /**
   * Init method to initialize the broker set to broker mapping and reverse mapping
   *
   * @param clusterModel cluster model
   * @param brokerSetResolver broker set resolver object
   * @throws BrokerSetResolutionException
   */
  private void initializeBrokerSetMappings(ClusterModel clusterModel, BrokerSetResolver brokerSetResolver)
      throws BrokerSetResolutionException {
    _brokersByBrokerSetId = brokerSetResolver.brokerIdsByBrokerSetId(clusterModel)
                                             .entrySet()
                                             .stream()
                                             .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()
                                                                                                .stream()
                                                                                                .map(id -> clusterModel.broker(id))
                                                                                                .filter(Objects::nonNull)
                                                                                                .collect(Collectors.toSet())));

    _brokerSetIdByBrokerId = new HashMap<>();

    _brokersByBrokerSetId.forEach((brokerSet, brokers) -> {
      brokers.forEach(broker -> _brokerSetIdByBrokerId.put(broker.id(), brokerSet));
    });
  }

  /**
   * Helper function to get Broker to BrokerSet mapping
   *
   * @return broker by broker set id Mapping
   */
  public Map<String, Set<Broker>> brokersByBrokerSetId() {
    return _brokersByBrokerSetId;
  }

  /**
   * Helper function to get brokerSet for a broker
   *
   * @param broker broker object
   * @return broker set id
   */
  public String brokerSetId(Broker broker) throws BrokerSetResolutionException {
    String brokerSetId = _brokerSetIdByBrokerId.get(broker.id());

    if (brokerSetId != null) {
      return brokerSetId;
    }

    throw new BrokerSetResolutionException(String.format("Failed to resolve BrokerSet for Broker %s", broker));
  }
}
