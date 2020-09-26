/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;


public class AddBrokerPlan extends MaintenancePlan {
  public static final byte PLAN_VERSION = 0;
  private final Set<Integer> _brokers;

  public AddBrokerPlan(long timeMs, int brokerId, Set<Integer> brokers) {
    super(MaintenanceEventType.ADD_BROKER, timeMs, brokerId);
    if (brokers == null || brokers.isEmpty()) {
      throw new IllegalArgumentException("Missing brokers for the plan.");
    }
    if (brokers.size() > Short.MAX_VALUE) {
      throw new IllegalArgumentException(String.format("Cannot add more than %d brokers (attempt: %d).",
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
    short numBrokersToAdd = (short) _brokers.size();
    ByteBuffer buffer = ByteBuffer.allocate(headerSize
                                            + Byte.BYTES /* plan version */
                                            + Long.BYTES /* timeMs */
                                            + Integer.BYTES /* broker id */
                                            + Short.BYTES /* number of brokers to add */
                                            + (Integer.BYTES * numBrokersToAdd) /* brokers to add */);
    buffer.position(headerSize);
    buffer.put(planVersion());
    buffer.putLong(timeMs());
    buffer.putInt(brokerId());
    buffer.putShort(numBrokersToAdd);
    for (Integer brokerToAdd : _brokers) {
      buffer.putInt(brokerToAdd);
    }
    return buffer;
  }

  /**
   * Deserialize given byte buffer to an {@link AddBrokerPlan}.
   *
   * @param buffer buffer to deserialize.
   * @return The {@link AddBrokerPlan} corresponding to the deserialized buffer.
   */
  public static AddBrokerPlan fromBuffer(ByteBuffer buffer) throws UnknownVersionException {
    byte version = buffer.get();
    if (version > PLAN_VERSION) {
      throw new UnknownVersionException("Cannot deserialize the plan for version " + version + ". Current version: " + PLAN_VERSION);
    }

    long timeMs = buffer.getLong();
    int brokerId = buffer.getInt();
    short numBrokersToAdd = buffer.getShort();
    Set<Integer> brokers = new HashSet<>(numBrokersToAdd);
    for (short i = 0; i < numBrokersToAdd; i++) {
      brokers.add(buffer.getInt());
    }

    return new AddBrokerPlan(timeMs, brokerId, brokers);
  }

  @Override
  public String toString() {
    return String.format("[%s] Brokers: %s, Source [timeMs: %d, broker: %d]", _maintenanceEventType, _brokers, _timeMs, _brokerId);
  }
}
