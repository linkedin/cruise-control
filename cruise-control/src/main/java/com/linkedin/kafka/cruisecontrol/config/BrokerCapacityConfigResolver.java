/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Map;
import org.apache.kafka.common.Configurable;


/**
 * The interface for getting the broker capacity. Users should implement this interface so Cruise Control can
 * optimize the cluster according to the capacity of each broker.
 */
public interface BrokerCapacityConfigResolver extends Configurable, AutoCloseable {
  /**
   * Get the capacity of a broker based on rack, host and broker id.
   * The map returned must contain all the resources defined in {@link Resource}. The units for each resource are:
   * DISK - MegaBytes
   * CPU - Percentage (0 - 100)
   * Network Inbound - KB/s
   * Network Outbounds - KB/s
   *
   * @param rack The rack of the broker
   * @param host The host of the broker
   * @param brokerId the id of the broker
   * @return The capacity of each resource for the broker
   */
  Map<Resource, Double> capacityForBroker(String rack, String host, int brokerId);
}
