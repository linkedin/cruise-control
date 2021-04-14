/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.common.utils.Crc32C;


public abstract class MaintenancePlanWithBrokers extends MaintenancePlan {
  protected final SortedSet<Integer> _brokers;

  public MaintenancePlanWithBrokers(MaintenanceEventType maintenanceEventType, long timeMs, int brokerId,
      byte planVersion, SortedSet<Integer> brokers) {
    super(maintenanceEventType, timeMs, brokerId, planVersion);

    if (brokers == null || brokers.isEmpty()) {
      throw new IllegalArgumentException("Missing brokers for the plan.");
    }
    _brokers = new TreeSet<>(brokers);
  }

  protected long getCrc() {
    short numBrokers = (short) _brokers.size();
    int contentSize = (Byte.BYTES /* maintenance event type id */
                       + Byte.BYTES /* plan version */
                       + Long.BYTES /* timeMs */
                       + Integer.BYTES /* broker id */
                       + Short.BYTES /* number of brokers */
                       + (Integer.BYTES * numBrokers) /* brokers */);
    ByteBuffer buffer = ByteBuffer.allocate(contentSize);
    buffer.put(maintenanceEventType().id());
    buffer.put(planVersion());
    buffer.putLong(timeMs());
    buffer.putInt(brokerId());
    buffer.putShort(numBrokers);
    for (Integer broker : _brokers) {
      buffer.putInt(broker);
    }
    // The CRC covers all data to the end of the buffer.
    return Crc32C.compute(buffer, -buffer.position(), contentSize);
  }

  public Set<Integer> brokers() {
    return _brokers;
  }

  @Override
  public String toString() {
    return String.format("[%s] Brokers: %s, Source [timeMs: %d, broker: %d]", _maintenanceEventType, _brokers, _timeMs, _brokerId);
  }
}
