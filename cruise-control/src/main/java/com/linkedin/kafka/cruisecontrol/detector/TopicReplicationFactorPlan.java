/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class TopicReplicationFactorPlan extends MaintenancePlan {
  public static final byte PLAN_VERSION = 0;
  // A map containing the regex of topics by the corresponding desired replication factor
  private final Map<Short, String> _topicRegexWithRFUpdate;

  public TopicReplicationFactorPlan(long timeMs, int brokerId, Map<Short, String> topicRegexWithRFUpdate) {
    super(MaintenanceEventType.TOPIC_REPLICATION_FACTOR, timeMs, brokerId);
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

  @Override
  public byte planVersion() {
    return PLAN_VERSION;
  }

  public Map<Short, String> topicRegexWithRFUpdate() {
    return _topicRegexWithRFUpdate;
  }

  @Override
  public ByteBuffer toBuffer(int headerSize) {
    byte numRFUpdateEntries = (byte) _topicRegexWithRFUpdate.size();
    int requiredCapacityForRFEntries = 0;
    for (Map.Entry<Short, String> entry : _topicRegexWithRFUpdate.entrySet()) {
      requiredCapacityForRFEntries += (Short.BYTES /* replication factor */ + Integer.BYTES /* regex length */
                                       + entry.getValue().getBytes(StandardCharsets.UTF_8).length /* regex */);
    }
    int contentSize = (Byte.BYTES /* plan version */
                       + Long.BYTES /* timeMs */
                       + Integer.BYTES /* broker id */
                       + Byte.BYTES /* number of replication factor update entries */
                       + requiredCapacityForRFEntries /* total capacity for all entries */);
    ByteBuffer buffer = ByteBuffer.allocate(headerSize + Long.BYTES /* crc */ + contentSize);
    buffer.position(headerSize + Long.BYTES);
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
    putCrc(headerSize, buffer, contentSize);
    return buffer;
  }

  /**
   * Deserialize given byte buffer to an {@link TopicReplicationFactorPlan}.
   *
   * @param headerSize The header size of the buffer.
   * @param buffer buffer to deserialize.
   * @return The {@link TopicReplicationFactorPlan} corresponding to the deserialized buffer.
   */
  public static TopicReplicationFactorPlan fromBuffer(int headerSize, ByteBuffer buffer) throws UnknownVersionException {
    verifyCrc(headerSize, buffer);
    byte version = buffer.get();
    if (version > PLAN_VERSION) {
      throw new UnknownVersionException("Cannot deserialize the plan for version " + version + ". Current version: " + PLAN_VERSION);
    }

    long timeMs = buffer.getLong();
    int brokerId = buffer.getInt();
    byte numRFUpdateEntries = buffer.get();
    Map<Short, String> topicRegexWithRFUpdate = new HashMap<>(numRFUpdateEntries);

    for (int i = 0; i < numRFUpdateEntries; i++) {
      short replicationFactor = buffer.getShort();
      int regexLength = buffer.getInt();
      String regex = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), regexLength, StandardCharsets.UTF_8);
      buffer.position(buffer.position() + regexLength);
      topicRegexWithRFUpdate.put(replicationFactor, regex);
    }

    return new TopicReplicationFactorPlan(timeMs, brokerId, topicRegexWithRFUpdate);
  }

  @Override
  public String toString() {
    return String.format("[%s] TopicRegexWithRFUpdate: %s, Source [timeMs: %d, broker: %d]", _maintenanceEventType,
                         _topicRegexWithRFUpdate, _timeMs, _brokerId);
  }
}
