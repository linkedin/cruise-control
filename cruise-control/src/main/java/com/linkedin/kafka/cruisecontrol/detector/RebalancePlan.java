/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;


public class RebalancePlan extends MaintenancePlan {
  public static final byte PLAN_VERSION = 0;

  public RebalancePlan(long timeMs, int brokerId) {
    super(MaintenanceEventType.REBALANCE, timeMs, brokerId);
  }

  @Override
  public byte planVersion() {
    return PLAN_VERSION;
  }

  /**
   * Deserialize given byte buffer to a {@link RebalancePlan}.
   *
   * @param headerSize The header size of the buffer.
   * @param buffer buffer to deserialize.
   * @return The {@link RebalancePlan} corresponding to the deserialized buffer.
   */
  public static RebalancePlan fromBuffer(int headerSize, ByteBuffer buffer) throws UnknownVersionException {
    verifyCrc(headerSize, buffer);
    byte version = buffer.get();
    if (version > PLAN_VERSION) {
      throw new UnknownVersionException("Cannot deserialize the plan for version " + version + ". Current version: " + PLAN_VERSION);
    }

    long timeMs = buffer.getLong();
    int brokerId = buffer.getInt();

    return new RebalancePlan(timeMs, brokerId);
  }

  @Override
  public String toString() {
    return String.format("[%s] Source [timeMs: %d, broker: %d]", _maintenanceEventType, _timeMs, _brokerId);
  }
}
