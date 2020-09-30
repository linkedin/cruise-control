/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.Crc32C;


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
   * Verifies the crc of the given buffer with the crc computed with the rest of the buffer.
   *
   * @param headerSize The header size of the buffer.
   * @param buffer The buffer, whose next eight bytes at its current position has the crc value to be verified.
   */
  protected static void verifyCrc(int headerSize, ByteBuffer buffer) {
    long crc = buffer.getLong();
    long computedCrc = Crc32C.compute(buffer, 0, buffer.limit() - (headerSize + Long.BYTES /* crc */));
    if (crc != computedCrc) {
      throw new IllegalArgumentException(String.format("The plan is corrupt. CRC (stored: %d, computed: %d)", crc, computedCrc));
    }
  }

  /**
   * Puts the crc to the current position of the given buffer.
   * The CRC covers all content to the end of the buffer -- i.e. all the bytes that follow the CRC.
   *
   * @param headerSize The header size of the buffer.
   * @param buffer The buffer that has all bytes that follow the CRC written to it.
   * @param contentSize The size of the content -- i.e. the size of all the bytes that follow the CRC.
   */
  protected void putCrc(int headerSize, ByteBuffer buffer, int contentSize) {
    // The CRC covers all data to the end of the buffer -- i.e. all the bytes that follow the CRC.
    long crc = Crc32C.compute(buffer, headerSize + Long.BYTES /* crc */ - buffer.position(), contentSize);
    buffer.putLong(buffer.position() - contentSize - Long.BYTES /* crc */, crc);
  }

  /**
   * Serialize the maintenance plan to a byte buffer with the header size reserved.
   *
   * @param headerSize the header size to reserve.
   * @return A {@link ByteBuffer} with header size reserved at the beginning.
   */
  public ByteBuffer toBuffer(int headerSize) {
    int contentSize = (Byte.BYTES /* plan version */
                       + Long.BYTES /* timeMs */
                       + Integer.BYTES /* broker id */);
    ByteBuffer buffer = ByteBuffer.allocate(headerSize + Long.BYTES /* crc */ + contentSize);

    buffer.position(headerSize + Long.BYTES);
    buffer.put(planVersion());
    buffer.putLong(timeMs());
    buffer.putInt(brokerId());
    putCrc(headerSize, buffer, contentSize);
    return buffer;
  }

  @Override
  public String toString() {
    return String.format("[%s] Source [timeMs: %d, broker: %d]", _maintenanceEventType, _timeMs, _brokerId);
  }
}
