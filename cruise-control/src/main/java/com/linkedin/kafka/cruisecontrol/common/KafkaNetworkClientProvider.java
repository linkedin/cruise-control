/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;


public class KafkaNetworkClientProvider implements NetworkClientProvider {

  public KafkaNetworkClientProvider() {

  }

  @Override
  public NetworkClient createNetworkClient(long connectionMaxIdleMs,
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
                                           ApiVersions apiVersions) {
    return new NetworkClient(new Selector(connectionMaxIdleMs, metrics, time, metricGrpPrefix, channelBuilder, new LogContext()),
                             metadata, clientId, maxInFlightRequestsPerConnection, reconnectBackoffMs,
                             reconnectBackoffMax, socketSendBuffer, socketReceiveBuffer, defaultRequestTimeoutMs,
                             ClientDnsLookup.DEFAULT, time, discoverBrokerVersions, apiVersions, new LogContext());
  }
}
