/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.nio.ByteBuffer;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.charset.StandardCharsets;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.*;


/**
 * The class hosting all the broker level metrics in {@link KafkaMetricDef}.
 */
public class BrokerMetricSample extends MetricSample<String, BrokerEntity> {
  private static final byte CURRENT_VERSION = 4;

  public BrokerMetricSample(String host, Integer brokerId) {
    super(new BrokerEntity(host, brokerId));
    if (host != null && host.length() >= Short.MAX_VALUE) {
      throw new IllegalArgumentException(String.format("The length of host name %s is %d, which is longer than "
                                                           + "the max allowed length of %d", host, host.length(),
                                                       Short.MAX_VALUE));
    }
  }

  public int brokerId() {
    return entity().brokerId();
  }

  /**
   * Serialize the partition metric sample using the following protocol
   *
   * 1 byte - version
   * 4 bytes - broker ID
   * 2 bytes - host name length
   * N bytes - host name
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
    byte[] hostBytes = (entity().group() != null ? entity().group() : "UNKNOWN").getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(297 + hostBytes.length);
    buffer.put(CURRENT_VERSION);
    buffer.putInt(entity().brokerId());
    buffer.putShort((short) hostBytes.length);
    buffer.put(hostBytes);
    buffer.putDouble(metricValue(CPU_USAGE));
    buffer.putDouble(metricValue(LEADER_BYTES_IN));
    buffer.putDouble(metricValue(LEADER_BYTES_OUT));
    buffer.putDouble(metricValue(REPLICATION_BYTES_IN_RATE));
    buffer.putDouble(metricValue(REPLICATION_BYTES_OUT_RATE));
    buffer.putDouble(metricValue(MESSAGE_IN_RATE));
    buffer.putDouble(metricValue(BROKER_PRODUCE_REQUEST_RATE));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_REQUEST_RATE));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_REQUEST_RATE));
    buffer.putDouble(metricValue(BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT));
    buffer.putDouble(metricValue(DISK_USAGE));
    buffer.putDouble(metricValue(PRODUCE_RATE));
    buffer.putDouble(metricValue(FETCH_RATE));
    buffer.putLong(_sampleTime);
    buffer.putInt(metricValue(BROKER_REQUEST_QUEUE_SIZE).intValue());
    buffer.putInt(metricValue(BROKER_RESPONSE_QUEUE_SIZE).intValue());
    buffer.putDouble(metricValue(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_PRODUCE_TOTAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_PRODUCE_LOCAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_LOG_FLUSH_RATE));
    buffer.putDouble(metricValue(BROKER_LOG_FLUSH_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_LOG_FLUSH_TIME_MS_MEAN));
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
      case 4:
        return readV4(buffer);
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  @Override
  protected MetricDef metricDefForToString() {
    return KafkaMetricDef.brokerMetricDef();
  }

  public Double metricFor(Resource resource) {
    switch (resource) {
      case CPU:
        return metricValue(CPU_USAGE);
      case DISK:
        return metricValue(DISK_USAGE);
      case NW_IN:
        return metricValue(LEADER_BYTES_IN) + metricValue(REPLICATION_BYTES_IN_RATE);
      case NW_OUT:
        return metricValue(LEADER_BYTES_OUT) + metricValue(REPLICATION_BYTES_OUT_RATE);
      default:
        throw new IllegalStateException("Should never happen");
    }
  }

  public Double metricValue(KafkaMetricDef kafkaMetricDef) {
    return _valuesByMetricId.get(KafkaMetricDef.brokerMetricDef().metricInfo(kafkaMetricDef.name()).id());
  }

  private static BrokerMetricSample readV0(ByteBuffer buffer) {
    MetricDef metricDef = KafkaMetricDef.brokerMetricDef();
    BrokerMetricSample brokerMetricSample = new BrokerMetricSample("UNKNOWN", -1);
    brokerMetricSample.record(metricDef.metricInfo(CPU_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), buffer.getDouble());
    return brokerMetricSample;
  }

  private static BrokerMetricSample readV1(ByteBuffer buffer) {
    MetricDef metricDef = KafkaMetricDef.brokerMetricDef();
    int brokerId = buffer.getInt();
    BrokerMetricSample brokerMetricSample = new BrokerMetricSample("UNKNOWN", brokerId);
    brokerMetricSample.record(metricDef.metricInfo(CPU_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(MESSAGE_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(DISK_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(PRODUCE_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(FETCH_RATE.name()), buffer.getDouble());
    long sampleTime = buffer.getLong();
    if (sampleTime >= 0) {
      brokerMetricSample.close(sampleTime);
    }
    return brokerMetricSample;
  }

  private static BrokerMetricSample readV2(ByteBuffer buffer) {
    MetricDef metricDef = KafkaMetricDef.brokerMetricDef();
    int brokerId = buffer.getInt();
    BrokerMetricSample brokerMetricSample = new BrokerMetricSample("UNKNOWN", brokerId);
    brokerMetricSample.record(metricDef.metricInfo(CPU_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(MESSAGE_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(DISK_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(PRODUCE_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(FETCH_RATE.name()), buffer.getDouble());
    long sampleTime = buffer.getLong();
    if (sampleTime >= 0) {
      brokerMetricSample.close(sampleTime);
    }
    return brokerMetricSample;
  }

  private static BrokerMetricSample readV3(ByteBuffer buffer) {
    MetricDef metricDef = KafkaMetricDef.brokerMetricDef();
    int brokerId = buffer.getInt();
    BrokerMetricSample brokerMetricSample = new BrokerMetricSample("UNKNOWN", brokerId);
    brokerMetricSample.record(metricDef.metricInfo(CPU_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(MESSAGE_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(DISK_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(PRODUCE_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(FETCH_RATE.name()), buffer.getDouble());
    long sampleTime = buffer.getLong();
    brokerMetricSample.record(metricDef.metricInfo(BROKER_REQUEST_QUEUE_SIZE.name()), buffer.getInt());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_RESPONSE_QUEUE_SIZE.name()), buffer.getInt());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());

    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_TIME_MS_MEAN.name()), buffer.getDouble());
    if (sampleTime >= 0) {
      brokerMetricSample.close(sampleTime);
    }
    return brokerMetricSample;
  }

  private static BrokerMetricSample readV4(ByteBuffer buffer) {
    MetricDef metricDef = KafkaMetricDef.brokerMetricDef();
    int brokerId = buffer.getInt();
    int hostLength = buffer.getShort();
    byte[] hostBytes = new byte[hostLength];
    buffer.get(hostBytes);
    String host = new String(hostBytes, StandardCharsets.UTF_8);
    BrokerMetricSample brokerMetricSample = new BrokerMetricSample(host, brokerId);

    brokerMetricSample.record(metricDef.metricInfo(CPU_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(MESSAGE_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(DISK_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(PRODUCE_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(FETCH_RATE.name()), buffer.getDouble());
    long sampleTime = buffer.getLong();
    brokerMetricSample.record(metricDef.metricInfo(BROKER_REQUEST_QUEUE_SIZE.name()), buffer.getInt());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_RESPONSE_QUEUE_SIZE.name()), buffer.getInt());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());

    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_TIME_MS_MEAN.name()), buffer.getDouble());
    if (sampleTime >= 0) {
      brokerMetricSample.close(sampleTime);
    }
    return brokerMetricSample;
  }
}
