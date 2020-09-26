/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;


public class FixOfflineReplicasPlan extends MaintenancePlan {
  public static final byte PLAN_VERSION = 0;

  public FixOfflineReplicasPlan(long timeMs, int brokerId) {
    super(MaintenanceEventType.FIX_OFFLINE_REPLICAS, timeMs, brokerId);
  }

  @Override
  public byte planVersion() {
    return PLAN_VERSION;
  }

  /**
   * Deserialize given byte buffer to an {@link FixOfflineReplicasPlan}.
   *
   * @param buffer buffer to deserialize.
   * @return The {@link FixOfflineReplicasPlan} corresponding to the deserialized buffer.
   */
  public static FixOfflineReplicasPlan fromBuffer(ByteBuffer buffer) throws UnknownVersionException {
    byte version = buffer.get();
    if (version > PLAN_VERSION) {
      throw new UnknownVersionException("Cannot deserialize the plan for version " + version + ". Current version: " + PLAN_VERSION);
    }

    long timeMs = buffer.getLong();
    int brokerId = buffer.getInt();

    return new FixOfflineReplicasPlan(timeMs, brokerId);
  }

  @Override
  public String toString() {
    return String.format("[%s] Source [timeMs: %d, broker: %d]", _maintenanceEventType, _timeMs, _brokerId);
  }
}
