/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.utils.Time;


@InterfaceStability.Evolving
public interface NetworkClientProvider {
  /**
   * Creates a new network client with the given properties.
   *
   * @return A new network client with the given properties.
   */
  NetworkClient createNetworkClient(long connectionMaxIdleMS,
                                    Metrics metrics,
                                    Time time,
                                    String metricGrpPrefix,
                                    ChannelBuilder channelBuilder,
                                    Metadata metadata,
                                    String clientId,
                                    int maxInFlightRequestsPerConnection,
                                    long reconnectBackoffMs,
                                    long reconnectBackoffMax,
                                    int socketSendBuffer,
                                    int socketReceiveBuffer,
                                    int defaultRequestTimeoutMs,
                                    boolean discoverBrokerVersions,
                                    ApiVersions apiVersions);
}
