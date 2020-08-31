/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.MAINTENANCE_EVENT;


public class MaintenanceEvent extends KafkaAnomaly {

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("Self healing for %s: %s", MAINTENANCE_EVENT, this);
  }

  @Override
  public AnomalyType anomalyType() {
    return MAINTENANCE_EVENT;
  }

  @Override
  public boolean fix() {
    // TODO: Start the relevant fix for the maintenance event.
    return false;
  }

  @Override
  public String toString() {
    // TODO: Add details on maintenance event.
    return super.toString();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    // TODO: Add configs for maintenance event.
  }
}
