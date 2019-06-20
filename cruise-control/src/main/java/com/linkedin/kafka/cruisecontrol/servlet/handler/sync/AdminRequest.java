/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AdminParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.AdminResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AdminRequest extends AbstractSyncRequest {
  private static final Logger LOG = LoggerFactory.getLogger(AdminRequest.class);
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final AdminParameters _parameters;

  public AdminRequest(KafkaCruiseControlServlet servlet, AdminParameters parameters) {
    super(servlet);
    _kafkaCruiseControl = servlet.asyncKafkaCruiseControl();
    _parameters = parameters;
  }

  /**
   * Handle the admin requests:
   * <ul>
   * <li>Dynamically change the partition and leadership concurrency of an ongoing execution. Has no effect if Executor
   * is in {@link com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State#NO_TASK_IN_PROGRESS} state.</li>
   * <li>Enable/disable the specified anomaly detectors.</li>
   * <li>Drop selected recently removed/demoted brokers.</li>
   * </ul>
   *
   * @return Admin response.
   */
  @Override
  protected AdminResult handle() {
    String ongoingConcurrencyChangeRequest = "";
    // 1.1. Change inter-broker partition concurrency.
    Integer concurrentInterBrokerPartitionMovements = _parameters.concurrentInterBrokerPartitionMovements();
    if (concurrentInterBrokerPartitionMovements != null) {
      _kafkaCruiseControl.setRequestedInterBrokerPartitionMovementConcurrency(concurrentInterBrokerPartitionMovements);
      ongoingConcurrencyChangeRequest += String.format("Inter-broker partition movement concurrency is set to %d%n",
                                                       concurrentInterBrokerPartitionMovements);
      LOG.info("Inter-broker partition movement concurrency is set to: {} by user.", concurrentInterBrokerPartitionMovements);
    }
    // 1.2. Change intra-broker partition concurrency.
    Integer concurrentIntraBrokerPartitionMovements = _parameters.concurrentIntraBrokerPartitionMovements();
    if (concurrentIntraBrokerPartitionMovements != null) {
      _kafkaCruiseControl.setRequestedIntraBrokerPartitionMovementConcurrency(concurrentIntraBrokerPartitionMovements);
      ongoingConcurrencyChangeRequest += String.format("Intra-broker partition movement concurrency is set to %d%n",
                                                       concurrentIntraBrokerPartitionMovements);
      LOG.info("Intra-broker partition movement concurrency is set to: {} by user.", concurrentIntraBrokerPartitionMovements);
    }
    // 1.3. Change leadership concurrency.
    Integer concurrentLeaderMovements = _parameters.concurrentLeaderMovements();
    if (concurrentLeaderMovements != null) {
      _kafkaCruiseControl.setRequestedLeadershipMovementConcurrency(concurrentLeaderMovements);
      ongoingConcurrencyChangeRequest += String.format("Leadership movement concurrency is set to %d%n", concurrentLeaderMovements);
      LOG.info("Leadership movement concurrency is set to: {} by user.", concurrentLeaderMovements);
    }

    // 2. Enable/disable the specified anomaly detectors
    Set<AnomalyType> disableSelfHealingFor = _parameters.disableSelfHealingFor();
    Set<AnomalyType> enableSelfHealingFor = _parameters.enableSelfHealingFor();

    Map<AnomalyType, Boolean>
        selfHealingBefore = new HashMap<>(disableSelfHealingFor.size() + enableSelfHealingFor.size());
    Map<AnomalyType, Boolean> selfHealingAfter = new HashMap<>(disableSelfHealingFor.size() + enableSelfHealingFor.size());

    for (AnomalyType anomalyType : disableSelfHealingFor) {
      selfHealingBefore.put(anomalyType, _kafkaCruiseControl.setSelfHealingFor(anomalyType, false));
      selfHealingAfter.put(anomalyType, false);
    }

    for (AnomalyType anomalyType : enableSelfHealingFor) {
      selfHealingBefore.put(anomalyType, _kafkaCruiseControl.setSelfHealingFor(anomalyType, true));
      selfHealingAfter.put(anomalyType, true);
    }

    if (!disableSelfHealingFor.isEmpty() || !enableSelfHealingFor.isEmpty()) {
      LOG.info("Self healing state is modified by user (before: {} after: {}).", selfHealingBefore, selfHealingAfter);
    }

    // 3. Drop selected recently removed/demoted brokers.
    String dropRecentBrokersRequest = processDropRecentBrokersRequest();

    return new AdminResult(selfHealingBefore,
                           selfHealingAfter,
                           ongoingConcurrencyChangeRequest.isEmpty() ? null : ongoingConcurrencyChangeRequest,
                           dropRecentBrokersRequest,
                           _kafkaCruiseControl.config());
  }

  private String processDropRecentBrokersRequest() {
    StringBuilder sb = new StringBuilder();

    Set<Integer> brokersToDropFromRecentlyRemoved = _parameters.dropRecentlyRemovedBrokers();
    if (!brokersToDropFromRecentlyRemoved.isEmpty()) {
      if (!_kafkaCruiseControl.dropRecentBrokers(brokersToDropFromRecentlyRemoved, true)) {
        Set<Integer> recentlyRemovedBrokers = _kafkaCruiseControl.recentBrokers(true);
        sb.append(String.format("None of the brokers to drop (%s) are in the recently removed broker set"
                                + " (%s).%n", brokersToDropFromRecentlyRemoved, recentlyRemovedBrokers));
        LOG.warn("None of the user-requested brokers to drop ({}) are in the recently removed broker set ({}).",
                 brokersToDropFromRecentlyRemoved, recentlyRemovedBrokers);
      } else {
        Set<Integer> recentlyRemovedBrokers = _kafkaCruiseControl.recentBrokers(true);
        sb.append(String.format("Dropped recently removed brokers (requested: %s after-dropping: %s).%n",
                                brokersToDropFromRecentlyRemoved, recentlyRemovedBrokers));
        LOG.info("Recently removed brokers are dropped by user (requested: {} after-dropping: {}).",
                 brokersToDropFromRecentlyRemoved, recentlyRemovedBrokers);
      }
    }

    Set<Integer> brokersToDropFromRecentlyDemoted = _parameters.dropRecentlyDemotedBrokers();
    if (!brokersToDropFromRecentlyDemoted.isEmpty()) {
      if (!_kafkaCruiseControl.dropRecentBrokers(brokersToDropFromRecentlyDemoted, false)) {
        Set<Integer> recentlyDemotedBrokers = _kafkaCruiseControl.recentBrokers(false);
        sb.append(String.format("None of the brokers to drop (%s) are in the recently demoted broker set"
                                + " (%s).%n", brokersToDropFromRecentlyDemoted, recentlyDemotedBrokers));
        LOG.warn("None of the user-requested brokers to drop ({}) are in the recently demoted broker set ({}).",
                 brokersToDropFromRecentlyDemoted, recentlyDemotedBrokers);
      } else {
        Set<Integer> recentlyDemotedBrokers = _kafkaCruiseControl.recentBrokers(false);
        sb.append(String.format("Dropped recently demoted brokers (requested: %s after-dropping: %s).%n",
                                brokersToDropFromRecentlyDemoted, recentlyDemotedBrokers));
        LOG.info("Recently demoted brokers are dropped by user (requested: {} after-dropping: {}).",
                 brokersToDropFromRecentlyDemoted, recentlyDemotedBrokers);
      }
    }

    return sb.toString();
  }

  @Override
  public AdminParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return AdminRequest.class.getSimpleName();
  }
}
