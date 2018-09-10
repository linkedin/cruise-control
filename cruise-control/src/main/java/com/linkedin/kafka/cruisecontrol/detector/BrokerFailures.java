/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;


/**
 * The broker failures that have been detected.
 */
public class BrokerFailures extends KafkaAnomaly {
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final Map<Integer, Long> _failedBrokers;
  private final boolean _allowCapacityEstimation;

  public BrokerFailures(KafkaCruiseControl kafkaCruiseControl,
                        Map<Integer, Long> failedBrokers,
                        boolean allowCapacityEstimation) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _failedBrokers = failedBrokers;
    _allowCapacityEstimation = allowCapacityEstimation;
  }

  /**
   * Get the failed broker list and their failure time in millisecond.
   */
  public Map<Integer, Long> failedBrokers() {
    return _failedBrokers;
  }

  @Override
  public void fix() throws KafkaCruiseControlException {
    // Fix the cluster by removing the failed brokers (mode: non-Kafka_assigner).
    if (_failedBrokers != null && !_failedBrokers.isEmpty()) {
      _kafkaCruiseControl.decommissionBrokers(_failedBrokers.keySet(), false, false,
                                             Collections.emptyList(), null, new OperationProgress(),
                                              _allowCapacityEstimation, null, null, false, null);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("{\n");
    _failedBrokers.forEach((key, value) -> {
      Date date = new Date(value);
      DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
      sb.append("\tBroker ").append(key).append(" failed at ").append(format.format(date)).append("\n");
    });
    sb.append("}");
    return sb.toString();
  }
}
