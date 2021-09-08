/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetadataClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataClient.class);
  private static final int DEFAULT_MAX_IN_FLIGHT_REQUEST = 1;
  private final AtomicInteger _metadataGeneration;
  private final Metadata _metadata;
  private final NetworkClient _networkClient;
  private final Time _time;
  private final long _metadataTTL;
  private final long _refreshMetadataTimeout;

  public MetadataClient(KafkaCruiseControlConfig config,
                        Metadata metadata,
                        long metadataTTL,
                        Time time) {
    _metadataGeneration = new AtomicInteger(0);
    _metadata = metadata;
    _refreshMetadataTimeout = config.getLong(MonitorConfig.METADATA_MAX_AGE_MS_CONFIG);
    _time = time;
    List<InetSocketAddress> addresses =
        ClientUtils.parseAndValidateAddresses(config.getList(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG),
                                              ClientDnsLookup.DEFAULT);
    Cluster bootstrapCluster = Cluster.bootstrap(addresses);
    MetadataResponse metadataResponse = KafkaCruiseControlUtils.prepareMetadataResponse(bootstrapCluster.nodes(),
                                                                                        bootstrapCluster.clusterResource().clusterId(),
                                                                                        MetadataResponse.NO_CONTROLLER_ID,
                                                                                        Collections.emptyList());

    _metadata.update(KafkaCruiseControlUtils.REQUEST_VERSION_UPDATE, metadataResponse, time.milliseconds());
    ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time);
    NetworkClientProvider provider = config.getConfiguredInstance(MonitorConfig.NETWORK_CLIENT_PROVIDER_CLASS_CONFIG,
                                                                  NetworkClientProvider.class);

    _networkClient = provider.createNetworkClient(config.getLong(MonitorConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                                                  new Metrics(),
                                                  time,
                                                  "load-monitor",
                                                  channelBuilder,
                                                  _metadata,
                                                  config.getString(MonitorConfig.CLIENT_ID_CONFIG),
                                                  DEFAULT_MAX_IN_FLIGHT_REQUEST,
                                                  config.getLong(MonitorConfig.RECONNECT_BACKOFF_MS_CONFIG),
                                                  config.getLong(MonitorConfig.RECONNECT_BACKOFF_MS_CONFIG),
                                                  config.getInt(MonitorConfig.SEND_BUFFER_CONFIG),
                                                  config.getInt(MonitorConfig.RECEIVE_BUFFER_CONFIG),
                                                  config.getInt(MonitorConfig.REQUEST_TIMEOUT_MS_CONFIG),
                                                  true,
                                                  new ApiVersions());
    _metadataTTL = metadataTTL;
    // Ensure that the initial metadata is valid.
    doRefreshMetadata(_refreshMetadataTimeout);
  }

  /**
   * Refresh the metadata. The method is synchronized because the network client is not thread safe.
   * @return A new {@link ClusterAndGeneration} with latest cluster and generation.
   */
  public synchronized ClusterAndGeneration refreshMetadata() {
    return refreshMetadata(_refreshMetadataTimeout);
  }

  /**
   * Refresh the metadata. The method is synchronized because the network client is not thread safe.
   * @param timeoutMs Timeout in milliseconds.
   * @return A new {@link ClusterAndGeneration} with latest cluster and generation.
   */
  public synchronized ClusterAndGeneration refreshMetadata(long timeoutMs) {
    // Do not update metadata if the metadata has just been refreshed.
    if (_time.milliseconds() >= _metadata.lastSuccessfulUpdate() + _metadataTTL) {
      doRefreshMetadata(timeoutMs);
    }
    return clusterAndGeneration();
  }

  private void doRefreshMetadata(long timeoutMs) {
    int updateVersion = _metadata.requestUpdate();
    long remaining = timeoutMs;
    Cluster beforeUpdate = cluster();
    boolean isMetadataUpdated = _metadata.updateVersion() > updateVersion;
    while (!isMetadataUpdated && remaining > 0) {
      _metadata.requestUpdate();
      long start = _time.milliseconds();
      _networkClient.poll(remaining, start);
      remaining -= (_time.milliseconds() - start);
      isMetadataUpdated = _metadata.updateVersion() > updateVersion;
    }
    if (isMetadataUpdated) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated metadata {}", cluster());
      }
      if (MonitorUtils.metadataChanged(beforeUpdate, cluster())) {
        _metadataGeneration.incrementAndGet();
      }
    } else {
      LOG.warn("Failed to update metadata in {}ms. Using old metadata with version {} and last successful update {}.",
               timeoutMs, _metadata.updateVersion(), _metadata.lastSuccessfulUpdate());
    }
  }

  /**
   * Close the metadata client. Synchronized to avoid interrupting the network client during a poll.
   */
  public synchronized void close() {
    _networkClient.close();
  }

  /**
   * @return The metadata maintained by this metadata client.
   */
  public Metadata metadata() {
    return _metadata;
  }

  /**
   * @return The current cluster and generation.
   */
  public ClusterAndGeneration clusterAndGeneration() {
    return new ClusterAndGeneration(cluster(), _metadataGeneration.get());
  }

  /**
   * @return The current cluster.
   */
  public Cluster cluster() {
    return _metadata.fetch();
  }

  public static class ClusterAndGeneration {
    private final Cluster _cluster;
    private final int _generation;

    public ClusterAndGeneration(Cluster cluster, int generation) {
      _cluster = cluster;
      _generation = generation;
    }

    /**
     * @return Kafka cluster.
     */
    public Cluster cluster() {
      return _cluster;
    }

    /**
     * @return Model generation of the cluster
     */
    public int generation() {
      return _generation;
    }
  }
}
