/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;

import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.TOPIC_ANOMALY;

/**
 * The interface for a topic anomaly.
 */
public abstract class TopicAnomaly extends KafkaAnomaly {
  @Override
  public AnomalyType anomalyType() {
    return TOPIC_ANOMALY;
  }
}
