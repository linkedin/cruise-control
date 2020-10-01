/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.utils.Crc32C;


/**
 * A plan to update the replication factor of specified topics.
 *
 * The desired replication factor for topics are indicated using topic regex per desired replication factor.
 */
public class TopicReplicationFactorPlan extends MaintenancePlan {
  public static final byte LATEST_SUPPORTED_VERSION = 0;
  // A map containing the regex of topics by the corresponding desired replication factor
  private final Map<Short, String> _topicRegexWithRFUpdate;

  public TopicReplicationFactorPlan(long timeMs, int brokerId, Map<Short, String> topicRegexWithRFUpdate) {
    super(MaintenanceEventType.TOPIC_REPLICATION_FACTOR, timeMs, brokerId, LATEST_SUPPORTED_VERSION);
    if (topicRegexWithRFUpdate == null || topicRegexWithRFUpdate.isEmpty()) {
      throw new IllegalArgumentException("Missing replication factor updates for the plan.");
    }
    // Sanity check: the number of bulk updates for replication factor of topics.
    if (topicRegexWithRFUpdate.size() > Byte.MAX_VALUE) {
      throw new IllegalArgumentException(String.format("Cannot update more than %d different replication factor (attempt: %d).",
                                                       Byte.MAX_VALUE, topicRegexWithRFUpdate.size()));
    }
    // Sanity check: Each regex must have some value.
    for (String regex : topicRegexWithRFUpdate.values()) {
      if (regex == null || regex.isEmpty()) {
        throw new IllegalArgumentException("Missing topics of the replication factor update for the plan.");
      }
    }
    _topicRegexWithRFUpdate = topicRegexWithRFUpdate;
  }

  public Map<Short, String> topicRegexWithRFUpdate() {
    return _topicRegexWithRFUpdate;
  }

  protected long getCrc() {
    byte numRFUpdateEntries = (byte) _topicRegexWithRFUpdate.size();
    int requiredCapacityForRFEntries = 0;
    for (Map.Entry<Short, String> entry : _topicRegexWithRFUpdate.entrySet()) {
      requiredCapacityForRFEntries += (Short.BYTES /* replication factor */ + Integer.BYTES /* regex length */
                                       + entry.getValue().getBytes(StandardCharsets.UTF_8).length /* regex */);
    }
    int contentSize = (Byte.BYTES /* maintenance event type id */
                       + Byte.BYTES /* plan version */
                       + Long.BYTES /* timeMs */
                       + Integer.BYTES /* broker id */
                       + Byte.BYTES /* number of replication factor update entries */
                       + requiredCapacityForRFEntries /* total capacity for all entries */);
    ByteBuffer buffer = ByteBuffer.allocate(contentSize);
    buffer.put(maintenanceEventType().id());
    buffer.put(planVersion());
    buffer.putLong(timeMs());
    buffer.putInt(brokerId());
    buffer.put(numRFUpdateEntries);
    for (Map.Entry<Short, String> entry : _topicRegexWithRFUpdate.entrySet()) {
      buffer.putShort(entry.getKey());
      byte[] regex = entry.getValue().getBytes(StandardCharsets.UTF_8);
      buffer.putInt(regex.length);
      buffer.put(regex);
    }
    // The CRC covers all data to the end of the buffer.
    return Crc32C.compute(buffer, -buffer.position(), contentSize);
  }

  @Override
  public String toString() {
    return String.format("[%s] TopicRegexWithRFUpdate: %s, Source [timeMs: %d, broker: %d]", _maintenanceEventType,
                         _topicRegexWithRFUpdate, _timeMs, _brokerId);
  }
}
