/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
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

  public MetadataClient(KafkaCruiseControlConfig config,
                        Metadata metadata,
                        long metadataTTL,
                        Time time) {
    _metadataGeneration = new AtomicInteger(0);
    _metadata = metadata;
    _time = time;
    List<InetSocketAddress> addresses =
        ClientUtils.parseAndValidateAddresses(config.getList(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG));
    _metadata.update(Cluster.bootstrap(addresses), time.milliseconds());
    ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
    _networkClient = new NetworkClient(
        new Selector(config.getLong(KafkaCruiseControlConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), new Metrics(), time, "load-monitor", channelBuilder),
        _metadata,
        config.getString(KafkaCruiseControlConfig.CLIENT_ID_CONFIG),
        DEFAULT_MAX_IN_FLIGHT_REQUEST,
        config.getLong(KafkaCruiseControlConfig.RECONNECT_BACKOFF_MS_CONFIG),
        config.getInt(KafkaCruiseControlConfig.SEND_BUFFER_CONFIG),
        config.getInt(KafkaCruiseControlConfig.RECEIVE_BUFFER_CONFIG),
        config.getInt(KafkaCruiseControlConfig.REQUEST_TIMEOUT_MS_CONFIG),
        _time);
    _metadataTTL = metadataTTL;
  }

  /**
   * Refresh the metadata. The method is synchronized because the network client is not thread safe.
   */
  public synchronized ClusterAndGeneration refreshMetadata() {
    return refreshMetadata(Long.MAX_VALUE);
  }

  /**
   * Refresh the metadata. The method is synchronized because the network client is not thread safe.
   */
  public synchronized ClusterAndGeneration refreshMetadata(long timeout) {
    // Do not update metadata if the metadata has just been refreshed.
    if (_metadataTTL <= 0 || (_time.milliseconds() >= _metadata.lastSuccessfulUpdate() + _metadataTTL)) {
      // This is a super confusing interface in the Metadata. If we don't set this to false, the metadata.update()
      // will remove all the topics that are not in the metadata interested topics list.
      _metadata.addListener(cluster -> _metadata.needMetadataForAllTopics(false));
      // Cruise Control always fetch metadata for all the topics.
      _metadata.needMetadataForAllTopics(true);
      int version = _metadata.requestUpdate();
      long remaining = timeout;
      Cluster beforeUpdate = _metadata.fetch();
      while (_metadata.version() <= version && remaining > 0) {
        _metadata.requestUpdate();
        long start = _time.milliseconds();
        _networkClient.poll(remaining, start);
        remaining -= (_time.milliseconds() - start);
      }
      LOG.debug("Updated metadata {}", _metadata.fetch());
      if (MonitorUtils.metadataChanged(beforeUpdate, _metadata.fetch())) {
        _metadataGeneration.incrementAndGet();
      }
    }
    return new ClusterAndGeneration(_metadata.fetch(), _metadataGeneration.get());
  }

  public ClusterAndGeneration clusterAndGeneration() {
    return new ClusterAndGeneration(_metadata.fetch(), _metadataGeneration.get());
  }

  public Cluster cluster() {
    return _metadata.fetch();
  }

  public static class ClusterAndGeneration {
    private final Cluster _cluster;
    private final int _generation;

    private ClusterAndGeneration(Cluster cluster, int generation) {
      _cluster = cluster;
      _generation = generation;
    }

    public Cluster cluster() {
      return _cluster;
    }

    public int generation() {
      return _generation;
    }
  }
}
