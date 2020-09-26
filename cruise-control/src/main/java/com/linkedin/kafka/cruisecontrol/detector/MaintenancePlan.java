/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.nio.ByteBuffer;


/**
 * An abstract class for all maintenance plans.
 */
public abstract class MaintenancePlan {
  protected final MaintenanceEventType _maintenanceEventType;
  protected final long _timeMs;
  protected final int _brokerId;

  public MaintenancePlan(MaintenanceEventType maintenanceEventType, long timeMs, int brokerId) {
    _maintenanceEventType = maintenanceEventType;
    _timeMs = timeMs;
    _brokerId = brokerId;
  }

  /**
   * Retrieve the maintenance event type of this maintenance plan.
   *
   * @return the maintenance event type of this maintenance plan, which is stored in the serialized metrics so
   * that the deserializer will know which class should be used to deserialize the data.
   */
  public MaintenanceEventType maintenanceEventType() {
    return _maintenanceEventType;
  }

  /**
   * @return The timestamp in ms that corresponds to the generation of this plan.
   */
  public long timeMs() {
    return _timeMs;
  }

  /**
   * @return The id of the broker that reported this maintenance event.
   */
  public int brokerId() {
    return _brokerId;
  }

  /**
   * @return The plan version for serialization/deserialization.
   */
  public abstract byte planVersion();

  /**
   * Serialize the maintenance plan to a byte buffer with the header size reserved.
   *
   * @param headerSize the header size to reserve.
   * @return A {@link ByteBuffer} with header size reserved at the beginning.
   */
  public ByteBuffer toBuffer(int headerSize) {
    ByteBuffer buffer = ByteBuffer.allocate(headerSize
                                            + Byte.BYTES /* plan version */
                                            + Long.BYTES /* timeMs */
                                            + Integer.BYTES /* broker id */);
    buffer.position(headerSize);
    buffer.put(planVersion());
    buffer.putLong(timeMs());
    buffer.putInt(brokerId());
    return buffer;
  }

  @Override
  public String toString() {
    return String.format("[%s] Source [timeMs: %d, broker: %d]", _maintenanceEventType, _timeMs, _brokerId);
  }
}
