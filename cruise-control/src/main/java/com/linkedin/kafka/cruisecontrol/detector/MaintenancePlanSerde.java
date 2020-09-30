/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;


/**
 * A serializer / deserializer of {@link MaintenancePlan}.
 */
public class MaintenancePlanSerde implements Serializer<MaintenancePlan>, Deserializer<MaintenancePlan> {
  // The overhead of the type bytes
  private static final int EVENT_TYPE_OFFSET = 0;
  private static final int HEADER_LENGTH = Byte.BYTES;

  /**
   * Serialize the Maintenance plan to a byte array.
   *
   * @param recordValue Maintenance plan to be serialized.
   * @return Serialized Maintenance plan as a byte array.
   */
  public static byte[] toBytes(MaintenancePlan recordValue) {
    ByteBuffer byteBuffer = recordValue.toBuffer(HEADER_LENGTH);
    byteBuffer.put(EVENT_TYPE_OFFSET, recordValue.maintenanceEventType().id());
    return byteBuffer.array();
  }

  /**
   * Deserialize from byte array to Maintenance plan.
   *
   * @param bytes Bytes array corresponding to Maintenance plan.
   * @return Deserialized byte array as Maintenance plan, or {@code null} if the maintenance event type is not recognized.
   */
  public static MaintenancePlan fromBytes(byte[] bytes) throws UnknownVersionException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    switch (MaintenanceEventType.forId(buffer.get())) {
      case ADD_BROKER:
        return AddBrokerPlan.fromBuffer(HEADER_LENGTH, buffer);
      case REMOVE_BROKER:
        return RemoveBrokerPlan.fromBuffer(HEADER_LENGTH, buffer);
      case FIX_OFFLINE_REPLICAS:
        return FixOfflineReplicasPlan.fromBuffer(HEADER_LENGTH, buffer);
      case REBALANCE:
        return RebalancePlan.fromBuffer(HEADER_LENGTH, buffer);
      case DEMOTE_BROKER:
        return DemoteBrokerPlan.fromBuffer(HEADER_LENGTH, buffer);
      case TOPIC_REPLICATION_FACTOR:
        return TopicReplicationFactorPlan.fromBuffer(HEADER_LENGTH, buffer);
      default:
        // This could happen when a new type of maintenance event is added but we are still running the old code.
        return null;
    }
  }

  @Override
  public MaintenancePlan deserialize(String topic, byte[] bytes) {
    try {
      return fromBytes(bytes);
    } catch (Exception e) {
      throw new RuntimeException("Error occurred while deserializing Maintenance plan.", e);
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String topic, MaintenancePlan recordValue) {
    return toBytes(recordValue);
  }

  @Override
  public void close() {

  }
}
