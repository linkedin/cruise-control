/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class TopicMetric extends CruiseControlMetric {
  private static final byte METRIC_VERSION = 0;
  protected String _topic;

  public TopicMetric(RawMetricType rawMetricType, long time, int brokerId, String topic, double value) {
    super(rawMetricType, time, brokerId, value);
    if (rawMetricType.metricScope() != RawMetricType.MetricScope.TOPIC) {
      throw new IllegalArgumentException(String.format("Cannot construct a TopicMetric for %s whose scope is %s",
                                                       rawMetricType, rawMetricType.metricScope()));
    }
    _topic = topic;
  }

  public MetricClassId metricClassId() {
    return MetricClassId.TOPIC_METRIC;
  }

  public String topic() {
    return _topic;
  }

  public ByteBuffer toBuffer(int headerPos) {
    byte[] topic = _topic.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(headerPos + 1 /* version */ + 1 /* raw metric type */ +
                                                Long.BYTES /* time */ + Integer.BYTES /* broker id */ +
                                                Integer.BYTES /* topic length */ + topic.length /* topic */ +
                                                Double.BYTES /* value */);
    buffer.position(headerPos);
    buffer.put(METRIC_VERSION);
    buffer.put(rawMetricType().id());
    buffer.putLong(time());
    buffer.putInt(brokerId());
    buffer.putInt(topic.length);
    buffer.put(topic);
    buffer.putDouble(value());
    return buffer;
  }

  static TopicMetric fromBuffer(ByteBuffer buffer) throws UnknownVersionException {
    byte version = buffer.get();
    if (version > METRIC_VERSION) {
      throw new UnknownVersionException("Cannot deserialize the topic metrics for version " + version + ". "
                                            + "Current version is " + METRIC_VERSION);
    }
    RawMetricType rawMetricType = RawMetricType.forId(buffer.get());
    long time = buffer.getLong();
    int brokerId = buffer.getInt();
    int topicLength = buffer.getInt();
    String topic = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), topicLength, StandardCharsets.UTF_8);
    buffer.position(buffer.position() + topicLength);
    double value = buffer.getDouble();
    return new TopicMetric(rawMetricType, time, brokerId, topic, value);
  }

  @Override
  public String toString() {
    return String.format("[%s,%s,time=%d,brokerId=%d,topic=%s,value=%.3f]",
                         MetricClassId.TOPIC_METRIC, rawMetricType(), time(), brokerId(), topic(), value());
  }
}
