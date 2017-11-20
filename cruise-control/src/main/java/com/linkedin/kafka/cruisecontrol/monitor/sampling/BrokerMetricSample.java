/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.nio.ByteBuffer;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;


public class BrokerMetricSample {
  private static final byte CURRENT_VERSION = 2;
  private final int _brokerId;
  private final double _brokerCpuUtil;
  private final double _brokerLeaderBytesInRate;
  private final double _brokerLeaderBytesOutRate;
  private final double _brokerReplicationBytesInRate;
  private final double _brokerReplicationBytesOutRate;
  private final double _brokerMessagesInRate;
  private final double _brokerProduceRequestRate;
  private final double _brokerConsumerFetchRequestRate;
  private final double _brokerReplicationFetchRequestRate;
  private final double _brokerRequestHandlerAvgIdlePercent;
  private final double _brokerDiskUtil;
  private final double _allTopicsProduceRequestRate;
  private final double _allTopicsFetchRequestRate;
  private final long _sampleTime;

  @Deprecated
  public BrokerMetricSample(double brokerCpuUtil,
                            double brokerLeaderBytesInRate,
                            double brokerLeaderBytesOutRate,
                            double brokerReplicationBytesInRate) {
    this(-1, brokerCpuUtil, brokerLeaderBytesInRate, brokerLeaderBytesOutRate, brokerReplicationBytesInRate,
         -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0, -1.0,
         -1.0, -1.0, -1L);
  }

  @Deprecated
  public BrokerMetricSample(int brokerId,
                            double brokerCpuUtil,
                            double brokerLeaderBytesInRate,
                            double brokerLeaderBytesOutRate,
                            double brokerReplicationBytesInRate,
                            double brokerReplicationBytesOutRate,
                            double brokerMessagesInRate,
                            double brokerProduceRequestRate,
                            double brokerConsumerFetchRequestRate,
                            double brokerReplicationFetchRequestRate,
                            double brokerDiskUtil,
                            double allTopicsProduceRequestRate,
                            double allTopicsFetchRequestRate,
                            long sampleTime) {
    this(brokerId, brokerCpuUtil, brokerLeaderBytesInRate, brokerLeaderBytesOutRate, brokerReplicationBytesInRate,
        brokerReplicationBytesOutRate, brokerMessagesInRate, brokerProduceRequestRate,
        brokerConsumerFetchRequestRate, brokerReplicationFetchRequestRate, -1.0, brokerDiskUtil,
        allTopicsProduceRequestRate, allTopicsFetchRequestRate, sampleTime);
  }

  public BrokerMetricSample(int brokerId,
                            double brokerCpuUtil,
                            double brokerLeaderBytesInRate,
                            double brokerLeaderBytesOutRate,
                            double brokerReplicationBytesInRate,
                            double brokerReplicationBytesOutRate,
                            double brokerMessagesInRate,
                            double brokerProduceRequestRate,
                            double brokerConsumerFetchRequestRate,
                            double brokerReplicationFetchRequestRate,
                            double brokerRequestHandlerAvgIdlePercent,
                            double brokerDiskUtil,
                            double allTopicsProduceRequestRate,
                            double allTopicsFetchRequestRate,
                            long sampleTime) {
    _brokerId = brokerId;
    _brokerCpuUtil = brokerCpuUtil;
    _brokerLeaderBytesInRate = brokerLeaderBytesInRate;
    _brokerLeaderBytesOutRate = brokerLeaderBytesOutRate;
    _brokerReplicationBytesInRate = brokerReplicationBytesInRate;
    _brokerReplicationBytesOutRate = brokerReplicationBytesOutRate;
    _brokerMessagesInRate = brokerMessagesInRate;
    _brokerProduceRequestRate = brokerProduceRequestRate;
    _brokerConsumerFetchRequestRate = brokerConsumerFetchRequestRate;
    _brokerReplicationFetchRequestRate = brokerReplicationFetchRequestRate;
    _brokerRequestHandlerAvgIdlePercent = brokerRequestHandlerAvgIdlePercent;
    _brokerDiskUtil = brokerDiskUtil;
    _allTopicsProduceRequestRate = allTopicsProduceRequestRate;
    _allTopicsFetchRequestRate = allTopicsFetchRequestRate;
    _sampleTime = sampleTime;
  }

  public int brokerId() {
    return _brokerId;
  }

  public double brokerCpuUtil() {
    return _brokerCpuUtil;
  }

  public double brokerLeaderBytesInRate() {
    return _brokerLeaderBytesInRate;
  }

  public double brokerLeaderBytesOutRate() {
    return _brokerLeaderBytesOutRate;
  }

  public double brokerReplicationBytesInRate() {
    return _brokerReplicationBytesInRate;
  }

  public double brokerReplicationBytesOutRate() {
    return _brokerReplicationBytesOutRate;
  }

  public double brokerMessagesInRate() {
    return _brokerMessagesInRate;
  }

  public double brokerProduceRequestRate() {
    return _brokerProduceRequestRate;
  }

  public double brokerConsumerFetchRequestRate() {
    return _brokerConsumerFetchRequestRate;
  }

  public double brokerReplicationFetchRequestRate() {
    return _brokerReplicationFetchRequestRate;
  }

  public double brokerRequestHandlerAvgIdlePercent() {
    return _brokerRequestHandlerAvgIdlePercent;
  }

  public double brokerDiskUtilization() {
    return _brokerDiskUtil;
  }

  public double allTopicsProduceRequestRate() {
    return _allTopicsProduceRequestRate;
  }

  public double allTopicsFetchRequestRate() {
    return _allTopicsFetchRequestRate;
  }

  /**
   * Serialize the partition metric sample using the following protocol
   *
   * 1 byte - version
   * 4 bytes - broker ID
   * 8 bytes - broker cpu utilization.
   * 8 bytes - broker leader bytes in rate
   * 8 bytes - broker leader bytes out rate
   * 8 bytes - broker replication bytes in rate
   * 8 bytes - broker replication bytes out rate
   * 8 bytes - broker messages in rate
   * 8 bytes - broker produce request rate
   * 8 bytes - broker consumer fetch request rate
   * 8 bytes - broker replication fetch request rate
   * 8 bytes - broker request handler average idle percent
   * 8 bytes - broker disk utilization
   * 8 bytes - all topics produce request rate
   * 8 bytes - all topics fetch request rate
   * 8 bytes - sample time
   * @return the serialized bytes.
   */
  public byte[] toBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(117);
    buffer.put(CURRENT_VERSION);
    buffer.putInt(_brokerId);
    buffer.putDouble(_brokerCpuUtil);
    buffer.putDouble(_brokerLeaderBytesInRate);
    buffer.putDouble(_brokerLeaderBytesOutRate);
    buffer.putDouble(_brokerReplicationBytesInRate);
    buffer.putDouble(_brokerReplicationBytesOutRate);
    buffer.putDouble(_brokerMessagesInRate);
    buffer.putDouble(_brokerProduceRequestRate);
    buffer.putDouble(_brokerConsumerFetchRequestRate);
    buffer.putDouble(_brokerReplicationFetchRequestRate);
    buffer.putDouble(_brokerRequestHandlerAvgIdlePercent);
    buffer.putDouble(_brokerDiskUtil);
    buffer.putDouble(_allTopicsProduceRequestRate);
    buffer.putDouble(_allTopicsFetchRequestRate);
    buffer.putLong(_sampleTime);
    return buffer.array();
  }

  /**
   * Deserialize the bytes to get a broker metric data.
   * @param bytes the bytes to deserialize.
   * @return the deserialized broker metric sample.
   * @throws UnknownVersionException
   */
  public static BrokerMetricSample fromBytes(byte[] bytes) throws UnknownVersionException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    byte version = buffer.get();
    if (version > CURRENT_VERSION) {
      throw new UnknownVersionException("The model broker metric sample version " + version + " is higher than the current"
          + "version " + CURRENT_VERSION);
    }

    switch (version) {
      case 0:
        return readV0(buffer);
      case 1:
        return readV1(buffer);
      case 2:
        return readV2(buffer);
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  @Override
  public String toString() {
    return String.format("{CPU=%f, LEADER_BYTES_IN_RATE=%f, LEADER_BYTES_OUT_RATE=%f, REPLICATION_BYTES_IN_RATE=%f, "
                             + "REPLICATION_BYTES_OUT_RATE=%f, MESSAGES_IN_RATE=%f, PRODUCE_REQUEST_RATE=%f, "
                             + "CONSUMER_FETCH_REQUEST_RATE=%f, REPLICATION_FETCH_REQUEST_RATE=%f, "
                             + "REQUEST_HANDLER_AVG_IDLE_PERCENT=%f, DISK_UTIL=%f, ALL_TOPICS_PRODUCE_REQUEST_RATE=%f, "
                             + "ALL_TOPICS_FETCH_REQUEST_RATE=%f, "
                             + "SAMPLE_TIME=%d",
                         _brokerCpuUtil, _brokerLeaderBytesInRate, _brokerLeaderBytesOutRate,
                         _brokerReplicationBytesInRate, _brokerReplicationBytesOutRate, _brokerMessagesInRate,
                         _brokerProduceRequestRate, _brokerConsumerFetchRequestRate, _brokerReplicationFetchRequestRate,
        _brokerRequestHandlerAvgIdlePercent, _brokerDiskUtil, _allTopicsProduceRequestRate,
                         _allTopicsFetchRequestRate, _sampleTime);
  }

  public Double metricFor(Resource resource) {
    switch (resource) {
      case CPU:
        return _brokerCpuUtil;
      case DISK:
        return _brokerDiskUtil;
      case NW_IN:
        return _brokerLeaderBytesInRate + _brokerReplicationBytesInRate;
      case NW_OUT:
        return _brokerLeaderBytesOutRate;
      default:
        throw new IllegalStateException("Should never happen");
    }
  }

  public long sampleTime() {
    return _sampleTime;
  }

  private static BrokerMetricSample readV0(ByteBuffer buffer) {
    double brokerCpuUtil = buffer.getDouble();
    double brokerLeaderBytesInRate = buffer.getDouble();
    double brokerLeaderBytesOutRate = buffer.getDouble();
    double brokerReplicationBytesInRate = buffer.getDouble();
    return new BrokerMetricSample(-1,
                                  brokerCpuUtil,
                                  brokerLeaderBytesInRate,
                                  brokerLeaderBytesOutRate,
                                  brokerReplicationBytesInRate,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1L);
  }

  private static BrokerMetricSample readV1(ByteBuffer buffer) {
    int brokerId = buffer.getInt();
    double brokerCpuUtil = buffer.getDouble();
    double brokerLeaderBytesInRate = buffer.getDouble();
    double brokerLeaderBytesOutRate = buffer.getDouble();
    double brokerReplicationBytesInRate = buffer.getDouble();
    double brokerReplicationBytesOutRate = buffer.getDouble();
    double brokerMessageInRate = buffer.getDouble();
    double brokerProduceRequestRate = buffer.getDouble();
    double brokerConsumerFetchRequestRate = buffer.getDouble();
    double brokerReplicationFetchRequestRate = buffer.getDouble();
    double brokerDiskUtil = buffer.getDouble();
    double allTopicsProduceRequestRate = buffer.getDouble();
    double allTopicsFetchRequestRate = buffer.getDouble();
    long sampleTime = buffer.getLong();
    return new BrokerMetricSample(brokerId,
                                  brokerCpuUtil,
                                  brokerLeaderBytesInRate,
                                  brokerLeaderBytesOutRate,
                                  brokerReplicationBytesInRate,
                                  brokerReplicationBytesOutRate,
                                  brokerMessageInRate,
                                  brokerProduceRequestRate,
                                  brokerConsumerFetchRequestRate,
                                  brokerReplicationFetchRequestRate,
                                  -1.0,
                                  brokerDiskUtil,
                                  allTopicsProduceRequestRate,
                                  allTopicsFetchRequestRate,
                                  sampleTime);
  }

  private static BrokerMetricSample readV2(ByteBuffer buffer) {
    int brokerId = buffer.getInt();
    double brokerCpuUtil = buffer.getDouble();
    double brokerLeaderBytesInRate = buffer.getDouble();
    double brokerLeaderBytesOutRate = buffer.getDouble();
    double brokerReplicationBytesInRate = buffer.getDouble();
    double brokerReplicationBytesOutRate = buffer.getDouble();
    double brokerMessageInRate = buffer.getDouble();
    double brokerProduceRequestRate = buffer.getDouble();
    double brokerConsumerFetchRequestRate = buffer.getDouble();
    double brokerReplicationFetchRequestRate = buffer.getDouble();
    double brokerRequestHandlerAvgIdlePercent = buffer.getDouble();
    double brokerDiskUtil = buffer.getDouble();
    double allTopicsProduceRequestRate = buffer.getDouble();
    double allTopicsFetchRequestRate = buffer.getDouble();
    long sampleTime = buffer.getLong();
    return new BrokerMetricSample(brokerId,
                                  brokerCpuUtil,
                                  brokerLeaderBytesInRate,
                                  brokerLeaderBytesOutRate,
                                  brokerReplicationBytesInRate,
                                  brokerReplicationBytesOutRate,
                                  brokerMessageInRate,
                                  brokerProduceRequestRate,
                                  brokerConsumerFetchRequestRate,
                                  brokerReplicationFetchRequestRate,
                                  brokerRequestHandlerAvgIdlePercent,
                                  brokerDiskUtil,
                                  allTopicsProduceRequestRate,
                                  allTopicsFetchRequestRate,
                                  sampleTime);
  }
}
