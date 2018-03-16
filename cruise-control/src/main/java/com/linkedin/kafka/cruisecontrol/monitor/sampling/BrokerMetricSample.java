/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.nio.ByteBuffer;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;


public class BrokerMetricSample {
  private static final byte CURRENT_VERSION = 3;
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
  private final int _brokerRequestQueueSize;
  private final int _brokerResponseQueueSize;
  private final double _brokerProduceRequestQueueTimeMsMax;
  private final double _brokerProduceRequestQueueTimeMsMean;
  private final double _brokerConsumerFetchRequestQueueTimeMsMax;
  private final double _brokerConsumerFetchRequestQueueTimeMsMean;
  private final double _brokerFollowerFetchRequestQueueTimeMsMax;
  private final double _brokerFollowerFetchRequestQueueTimeMsMean;
  private final double _brokerProduceTotalTimeMsMax;
  private final double _brokerProduceTotalTimeMsMean;
  private final double _brokerConsumerFetchTotalTimeMsMax;
  private final double _brokerConsumerFetchTotalTimeMsMean;
  private final double _brokerFollowerFetchTotalTimeMsMax;
  private final double _brokerFollowerFetchTotalTimeMsMean;
  private final double _brokerProduceLocalTimeMsMax;
  private final double _brokerProduceLocalTimeMsMean;
  private final double _brokerConsumerFetchLocalTimeMsMax;
  private final double _brokerConsumerFetchLocalTimeMsMean;
  private final double _brokerFollowerFetchLocalTimeMsMax;
  private final double _brokerFollowerFetchLocalTimeMsMean;
  private final double _brokerLogFlushRate;
  private final double _brokerLogFlushTimeMsMax;
  private final double _brokerLogFlushTimeMsMean;

  @Deprecated
  public BrokerMetricSample(double brokerCpuUtil,
                            double brokerLeaderBytesInRate,
                            double brokerLeaderBytesOutRate,
                            double brokerReplicationBytesInRate) {
    this(-1, brokerCpuUtil, brokerLeaderBytesInRate, brokerLeaderBytesOutRate, brokerReplicationBytesInRate,
         -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0, -1.0,
         -1.0, -1.0, -1L, -1,
         -1, -1, -1,
         -1, -1, -1,
         -1, -1.0, -1.0,
         -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0);
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
        allTopicsProduceRequestRate, allTopicsFetchRequestRate, sampleTime, -1,
         -1, -1, -1,
         -1, -1, -1,
         -1, -1.0, -1.0,
         -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0);
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
                            double brokerRequestHandlerAvgIdlePercent,
                            double brokerDiskUtil,
                            double allTopicsProduceRequestRate,
                            double allTopicsFetchRequestRate,
                            long sampleTime) {
    this(brokerId, brokerCpuUtil, brokerLeaderBytesInRate, brokerLeaderBytesOutRate, brokerReplicationBytesInRate,
         brokerReplicationBytesOutRate, brokerMessagesInRate, brokerProduceRequestRate,
         brokerConsumerFetchRequestRate, brokerReplicationFetchRequestRate, brokerRequestHandlerAvgIdlePercent, brokerDiskUtil,
         allTopicsProduceRequestRate, allTopicsFetchRequestRate, sampleTime, -1,
         -1, -1, -1,
         -1, -1, -1,
         -1, -1.0, -1.0,
         -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0,
         -1.0, -1.0, -1.0);
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
                            long sampleTime,
                            int brokerRequestQueueSize,
                            int brokerResponseQueueSize,
                            double brokerProduceRequestQueueTimeMsMax,
                            double brokerProduceRequestQueueTimeMsMean,
                            double brokerConsumerFetchRequestQueueTimeMsMax,
                            double brokerConsumerFetchRequestQueueTimeMsMean,
                            double brokerFollowerFetchRequestQueueTimeMsMax,
                            double brokerFollowerFetchRequestQueueTimeMsMean,
                            double brokerProduceTotalTimeMsMax,
                            double brokerProduceTotalTimeMsMean,
                            double brokerConsumerFetchTotalTimeMsMax,
                            double brokerConsumerFetchTotalTimeMsMean,
                            double brokerFollowerFetchTotalTimeMsMax,
                            double brokerFollowerFetchTotalTimeMsMean,
                            double brokerProduceLocalTimeMsMax,
                            double brokerProduceLocalTimeMsMean,
                            double brokerConsumerFetchLocalTimeMsMax,
                            double brokerConsumerFetchLocalTimeMsMean,
                            double brokerFollowerFetchLocalTimeMsMax,
                            double brokerFollowerFetchLocalTimeMsMean,
                            double brokerLogFlushRate,
                            double brokerLogFlushTimeMsMax,
                            double brokerLogFlushTimeMsMean) {
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
    _brokerRequestQueueSize = brokerRequestQueueSize;
    _brokerResponseQueueSize = brokerResponseQueueSize;
    _brokerProduceRequestQueueTimeMsMax = brokerProduceRequestQueueTimeMsMax;
    _brokerProduceRequestQueueTimeMsMean = brokerProduceRequestQueueTimeMsMean;
    _brokerConsumerFetchRequestQueueTimeMsMax = brokerConsumerFetchRequestQueueTimeMsMax;
    _brokerConsumerFetchRequestQueueTimeMsMean = brokerConsumerFetchRequestQueueTimeMsMean;
    _brokerFollowerFetchRequestQueueTimeMsMax = brokerFollowerFetchRequestQueueTimeMsMax;
    _brokerFollowerFetchRequestQueueTimeMsMean = brokerFollowerFetchRequestQueueTimeMsMean;
    _brokerProduceTotalTimeMsMax = brokerProduceTotalTimeMsMax;
    _brokerProduceTotalTimeMsMean = brokerProduceTotalTimeMsMean;
    _brokerConsumerFetchTotalTimeMsMax = brokerConsumerFetchTotalTimeMsMax;
    _brokerConsumerFetchTotalTimeMsMean = brokerConsumerFetchTotalTimeMsMean;
    _brokerFollowerFetchTotalTimeMsMax = brokerFollowerFetchTotalTimeMsMax;
    _brokerFollowerFetchTotalTimeMsMean = brokerFollowerFetchTotalTimeMsMean;
    _brokerProduceLocalTimeMsMax = brokerProduceLocalTimeMsMax;
    _brokerProduceLocalTimeMsMean = brokerProduceLocalTimeMsMean;
    _brokerConsumerFetchLocalTimeMsMax = brokerConsumerFetchLocalTimeMsMax;
    _brokerConsumerFetchLocalTimeMsMean = brokerConsumerFetchLocalTimeMsMean;
    _brokerFollowerFetchLocalTimeMsMax = brokerFollowerFetchLocalTimeMsMax;
    _brokerFollowerFetchLocalTimeMsMean = brokerFollowerFetchLocalTimeMsMean;
    _brokerLogFlushRate = brokerLogFlushRate;
    _brokerLogFlushTimeMsMax = brokerLogFlushTimeMsMax;
    _brokerLogFlushTimeMsMean = brokerLogFlushTimeMsMean;
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

  public int requestQueueSize() {
    return _brokerRequestQueueSize;
  }

  public int responseQueueSize() {
    return _brokerResponseQueueSize;
  }

  public double produceRequestQueueTimeMsMax() {
    return _brokerProduceRequestQueueTimeMsMax;
  }

  public double produceRequestQueueTimeMsMean() {
    return _brokerProduceRequestQueueTimeMsMean;
  }

  public double consumerFetchRequestQueueTimeMsMax() {
    return _brokerConsumerFetchRequestQueueTimeMsMax;
  }

  public double consumerFetchRequestQueueTimeMsMean() {
    return _brokerConsumerFetchRequestQueueTimeMsMean;
  }

  public double followerFetchRequestQueueTimeMsMax() {
    return _brokerFollowerFetchRequestQueueTimeMsMax;
  }

  public double followerFetchRequestQueueTimeMsMean() {
    return _brokerFollowerFetchRequestQueueTimeMsMean;
  }

  public double produceTotalTimeMsMax() {
    return _brokerProduceTotalTimeMsMax;
  }

  public double produceTotalTimeMsMean() {
    return _brokerProduceTotalTimeMsMean;
  }

  public double consumerFetchTotalTimeMsMax() {
    return _brokerConsumerFetchTotalTimeMsMax;
  }

  public double consumerFetchTotalTimeMsMean() {
    return _brokerConsumerFetchTotalTimeMsMean;
  }

  public double followerFetchTotalTimeMsMax() {
    return _brokerFollowerFetchTotalTimeMsMax;
  }

  public double followerFetchTotalTimeMsMean() {
    return _brokerFollowerFetchTotalTimeMsMean;
  }

  public double produceLocalTimeMsMax() {
    return _brokerProduceLocalTimeMsMax;
  }

  public double produceLocalTimeMsMean() {
    return _brokerProduceLocalTimeMsMean;
  }

  public double consumerFetchLocalTimeMsMax() {
    return _brokerConsumerFetchLocalTimeMsMax;
  }

  public double consumerFetchLocalTimeMsMean() {
    return _brokerConsumerFetchLocalTimeMsMean;
  }

  public double followerFetchLocalTimeMsMax() {
    return _brokerFollowerFetchLocalTimeMsMax;
  }

  public double followerFetchLocalTimeMsMean() {
    return _brokerFollowerFetchLocalTimeMsMean;
  }

  public double logFlushRate() {
    return _brokerLogFlushRate;
  }

  public double logFlushTimeMsMax() {
    return _brokerLogFlushTimeMsMax;
  }

  public double logFlushTimeMsMean() {
    return _brokerLogFlushTimeMsMean;
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
   * 4 bytes - broker request queue size
   * 4 bytes - broker response queue size
   * 8 bytes - broker produce request queue time ms (max)
   * 8 bytes - broker produce request queue time ms (mean)
   * 8 bytes - broker consumer fetch request queue time ms (max)
   * 8 bytes - broker consumer fetch request queue time ms (mean)
   * 8 bytes - broker follower fetch request queue time ms (max)
   * 8 bytes - broker follower fetch request queue time ms (mean)
   * 8 bytes - broker produce total time ms (max)
   * 8 bytes - broker produce total time ms (mean)
   * 8 bytes - broker consumer fetch total time ms (max)
   * 8 bytes - broker consumer fetch total time ms (mean)
   * 8 bytes - broker follower fetch total time ms (max)
   * 8 bytes - broker follower fetch total time ms (mean)
   * 8 bytes - broker produce local time ms (max)
   * 8 bytes - broker produce local time ms (mean)
   * 8 bytes - broker consumer fetch local time ms (max)
   * 8 bytes - broker consumer fetch local time ms (mean)
   * 8 bytes - broker follower fetch local time ms (max)
   * 8 bytes - broker follower fetch local time ms (mean)
   * 8 bytes - broker log flush rate
   * 8 bytes - broker log flush time ms (max)
   * 8 bytes - broker log flush time ms (mean)
   * @return the serialized bytes.
   */
  public byte[] toBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(293);
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
    buffer.putInt(_brokerRequestQueueSize);
    buffer.putInt(_brokerResponseQueueSize);
    buffer.putDouble(_brokerProduceRequestQueueTimeMsMax);
    buffer.putDouble(_brokerProduceRequestQueueTimeMsMean);
    buffer.putDouble(_brokerConsumerFetchRequestQueueTimeMsMax);
    buffer.putDouble(_brokerConsumerFetchRequestQueueTimeMsMean);
    buffer.putDouble(_brokerFollowerFetchRequestQueueTimeMsMax);
    buffer.putDouble(_brokerFollowerFetchRequestQueueTimeMsMean);
    buffer.putDouble(_brokerProduceTotalTimeMsMax);
    buffer.putDouble(_brokerProduceTotalTimeMsMean);
    buffer.putDouble(_brokerConsumerFetchTotalTimeMsMax);
    buffer.putDouble(_brokerConsumerFetchTotalTimeMsMean);
    buffer.putDouble(_brokerFollowerFetchTotalTimeMsMax);
    buffer.putDouble(_brokerFollowerFetchTotalTimeMsMean);
    buffer.putDouble(_brokerProduceLocalTimeMsMax);
    buffer.putDouble(_brokerProduceLocalTimeMsMean);
    buffer.putDouble(_brokerConsumerFetchLocalTimeMsMax);
    buffer.putDouble(_brokerConsumerFetchLocalTimeMsMean);
    buffer.putDouble(_brokerFollowerFetchLocalTimeMsMax);
    buffer.putDouble(_brokerFollowerFetchLocalTimeMsMean);
    buffer.putDouble(_brokerLogFlushRate);
    buffer.putDouble(_brokerLogFlushTimeMsMax);
    buffer.putDouble(_brokerLogFlushTimeMsMean);
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
      case 3:
        return readV3(buffer);
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
                         + "ALL_TOPICS_FETCH_REQUEST_RATE=%f, SAMPLE_TIME=%d, REQUEST_QUEUE_SIZE=%d, RESPONSE_QUEUE_SIZE=%d, "
                         + "PRODUCE_REQUEST_QUEUE_TIME_MS_MAX=%f, PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN=%f, "
                         + "CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX=%f, CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN=%f, "
                         + "FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX=%f, FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN=%f, "
                         + "PRODUCE_TOTAL_TIME_MS_MAX=%f, PRODUCE_TOTAL_TIME_MS_MEAN=%f, "
                         + "CONSUMER_FETCH_TOTAL_TIME_MS_MAX=%f, CONSUMER_FETCH_TOTAL_TIME_MS_MEAN=%f, "
                         + "FOLLOWER_FETCH_TOTAL_TIME_MS_MAX=%f, FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN=%f, "
                         + "PRODUCE_LOCAL_TIME_MS_MAX=%f, PRODUCE_LOCAL_TIME_MS_MEAN=%f, "
                         + "CONSUMER_FETCH_LOCAL_TIME_MS_MAX=%f, CONSUMER_FETCH_LOCAL_TIME_MS_MEAN=%f, "
                         + "FOLLOWER_FETCH_LOCAL_TIME_MS_MAX=%f, FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN=%f, "
                         + "LOG_FLUSH_RATE=%f, LOG_FLUSH_TIME_MS_MAX=%f, LOG_FLUSH_TIME_MS_MEAN=%f",
                         _brokerCpuUtil, _brokerLeaderBytesInRate, _brokerLeaderBytesOutRate,
                         _brokerReplicationBytesInRate, _brokerReplicationBytesOutRate, _brokerMessagesInRate,
                         _brokerProduceRequestRate, _brokerConsumerFetchRequestRate, _brokerReplicationFetchRequestRate,
                         _brokerRequestHandlerAvgIdlePercent, _brokerDiskUtil, _allTopicsProduceRequestRate,
                         _allTopicsFetchRequestRate, _sampleTime, _brokerRequestQueueSize, _brokerResponseQueueSize,
                         _brokerProduceRequestQueueTimeMsMax, _brokerProduceRequestQueueTimeMsMean,
                         _brokerConsumerFetchRequestQueueTimeMsMax, _brokerConsumerFetchRequestQueueTimeMsMean,
                         _brokerFollowerFetchRequestQueueTimeMsMax, _brokerFollowerFetchRequestQueueTimeMsMean,
                         _brokerProduceTotalTimeMsMax, _brokerProduceTotalTimeMsMean,
                         _brokerConsumerFetchTotalTimeMsMax, _brokerConsumerFetchTotalTimeMsMean,
                         _brokerFollowerFetchTotalTimeMsMax, _brokerFollowerFetchTotalTimeMsMean,
                         _brokerProduceLocalTimeMsMax, _brokerProduceLocalTimeMsMean,
                         _brokerConsumerFetchLocalTimeMsMax, _brokerConsumerFetchLocalTimeMsMean,
                         _brokerFollowerFetchLocalTimeMsMax, _brokerFollowerFetchLocalTimeMsMean, _brokerLogFlushRate,
                         _brokerLogFlushTimeMsMax, _brokerLogFlushTimeMsMean);
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
                                  -1L,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0);
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
                                  sampleTime,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0);
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
                                  sampleTime,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0,
                                  -1.0);
  }

  private static BrokerMetricSample readV3(ByteBuffer buffer) {
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
    int brokerRequestQueueSize = buffer.getInt();
    int brokerResponseQueueSize = buffer.getInt();
    double brokerProduceRequestQueueTimeMsMax = buffer.getDouble();
    double brokerProduceRequestQueueTimeMsMean = buffer.getDouble();
    double brokerConsumerFetchRequestQueueTimeMsMax = buffer.getDouble();
    double brokerConsumerFetchRequestQueueTimeMsMean = buffer.getDouble();
    double brokerFollowerFetchRequestQueueTimeMsMax = buffer.getDouble();
    double brokerFollowerFetchRequestQueueTimeMsMean = buffer.getDouble();
    double brokerProduceTotalTimeMsMax = buffer.getDouble();
    double brokerProduceTotalTimeMsMean = buffer.getDouble();
    double brokerConsumerFetchTotalTimeMsMax = buffer.getDouble();
    double brokerConsumerFetchTotalTimeMsMean = buffer.getDouble();
    double brokerFollowerFetchTotalTimeMsMax = buffer.getDouble();
    double brokerFollowerFetchTotalTimeMsMean = buffer.getDouble();
    double brokerProduceLocalTimeMsMax = buffer.getDouble();
    double brokerProduceLocalTimeMsMean = buffer.getDouble();
    double brokerConsumerFetchLocalTimeMsMax = buffer.getDouble();
    double brokerConsumerFetchLocalTimeMsMean = buffer.getDouble();
    double brokerFollowerFetchLocalTimeMsMax = buffer.getDouble();
    double brokerFollowerFetchLocalTimeMsMean = buffer.getDouble();
    double brokerLogFlushRate = buffer.getDouble();
    double brokerLogFlushTimeMsMax = buffer.getDouble();
    double brokerLogFlushTimeMsMean = buffer.getDouble();
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
                                  sampleTime,
                                  brokerRequestQueueSize,
                                  brokerResponseQueueSize,
                                  brokerProduceRequestQueueTimeMsMax,
                                  brokerProduceRequestQueueTimeMsMean,
                                  brokerConsumerFetchRequestQueueTimeMsMax,
                                  brokerConsumerFetchRequestQueueTimeMsMean,
                                  brokerFollowerFetchRequestQueueTimeMsMax,
                                  brokerFollowerFetchRequestQueueTimeMsMean,
                                  brokerProduceTotalTimeMsMax,
                                  brokerProduceTotalTimeMsMean,
                                  brokerConsumerFetchTotalTimeMsMax,
                                  brokerConsumerFetchTotalTimeMsMean,
                                  brokerFollowerFetchTotalTimeMsMax,
                                  brokerFollowerFetchTotalTimeMsMean,
                                  brokerProduceLocalTimeMsMax,
                                  brokerProduceLocalTimeMsMean,
                                  brokerConsumerFetchLocalTimeMsMax,
                                  brokerConsumerFetchLocalTimeMsMean,
                                  brokerFollowerFetchLocalTimeMsMax,
                                  brokerFollowerFetchLocalTimeMsMean,
                                  brokerLogFlushRate,
                                  brokerLogFlushTimeMsMax,
                                  brokerLogFlushTimeMsMean);
  }
}
