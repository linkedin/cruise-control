/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;


/**
 * The broker failures that have been detected.
 */
public class BrokerFailures extends Anomaly {
  private final Map<Integer, Long> _failedBrokers;

  public BrokerFailures(Map<Integer, Long> failedBrokers) {
    _failedBrokers = failedBrokers;
  }

  /**
   * Get the failed broker list and their failure time in millisecond.
   */
  public Map<Integer, Long> failedBrokers() {
    return _failedBrokers;
  }

  @Override
  void fix(KafkaCruiseControl kafkaCruiseControl) throws KafkaCruiseControlException {
    // Fix the cluster by removing the failed brokers.
    if (_failedBrokers != null && !_failedBrokers.isEmpty()) {
      kafkaCruiseControl.decommissionBrokers(_failedBrokers.keySet(), false, false,
                                             Collections.emptyList(), null);
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
