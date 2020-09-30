/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;


public class RemoveBrokerPlan extends MaintenancePlan {
  public static final byte PLAN_VERSION = 0;
  private final Set<Integer> _brokers;

  public RemoveBrokerPlan(long timeMs, int brokerId, Set<Integer> brokers) {
    super(MaintenanceEventType.REMOVE_BROKER, timeMs, brokerId);
    if (brokers == null || brokers.isEmpty()) {
      throw new IllegalArgumentException("Missing brokers for the plan.");
    }
    if (brokers.size() > Short.MAX_VALUE) {
      throw new IllegalArgumentException(String.format("Cannot remove more than %d brokers (attempt: %d).",
                                                       Short.MAX_VALUE, brokers.size()));
    }
    _brokers = brokers;
  }

  @Override
  public byte planVersion() {
    return PLAN_VERSION;
  }

  public Set<Integer> brokers() {
    return _brokers;
  }

  @Override
  public ByteBuffer toBuffer(int headerSize) {
    short numBrokersToRemove = (short) _brokers.size();
    int contentSize = (Byte.BYTES /* plan version */
                       + Long.BYTES /* timeMs */
                       + Integer.BYTES /* broker id */
                       + Short.BYTES /* number of brokers to remove */
                       + (Integer.BYTES * numBrokersToRemove) /* brokers to remove */);
    ByteBuffer buffer = ByteBuffer.allocate(headerSize + Long.BYTES /* crc */ + contentSize);
    buffer.position(headerSize + Long.BYTES);
    buffer.put(planVersion());
    buffer.putLong(timeMs());
    buffer.putInt(brokerId());
    buffer.putShort(numBrokersToRemove);
    for (Integer brokerToRemove : _brokers) {
      buffer.putInt(brokerToRemove);
    }
    putCrc(headerSize, buffer, contentSize);
    return buffer;
  }

  /**
   * Deserialize given byte buffer to an {@link RemoveBrokerPlan}.
   *
   * @param headerSize The header size of the buffer.
   * @param buffer buffer to deserialize.
   * @return The {@link RemoveBrokerPlan} corresponding to the deserialized buffer.
   */
  public static RemoveBrokerPlan fromBuffer(int headerSize, ByteBuffer buffer) throws UnknownVersionException {
    verifyCrc(headerSize, buffer);
    byte version = buffer.get();
    if (version > PLAN_VERSION) {
      throw new UnknownVersionException("Cannot deserialize the plan for version " + version + ". Current version: " + PLAN_VERSION);
    }

    long timeMs = buffer.getLong();
    int brokerId = buffer.getInt();
    short numBrokersToRemove = buffer.getShort();
    Set<Integer> brokers = new HashSet<>(numBrokersToRemove);
    for (short i = 0; i < numBrokersToRemove; i++) {
      brokers.add(buffer.getInt());
    }

    return new RemoveBrokerPlan(timeMs, brokerId, brokers);
  }

  @Override
  public String toString() {
    return String.format("[%s] Brokers: %s, Source [timeMs: %d, broker: %d]", _maintenanceEventType, _brokers, _timeMs, _brokerId);
  }
}