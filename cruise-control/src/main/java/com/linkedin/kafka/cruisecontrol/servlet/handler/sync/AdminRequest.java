/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AdminParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ChangeExecutionConcurrencyParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DropRecentBrokersParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UpdateConcurrencyAdjusterParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UpdateSelfHealingParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.AdminResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ADMIN_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class AdminRequest extends AbstractSyncRequest {
  private static final Logger LOG = LoggerFactory.getLogger(AdminRequest.class);
  protected KafkaCruiseControl _kafkaCruiseControl;
  protected AdminParameters _parameters;

  public AdminRequest() {
    super();
  }

  /**
   * Handle the admin requests:
   * <ul>
   * <li>Dynamically change the partition and leadership concurrency and the interval between checking and updating
   * (if needed) the progress of an ongoing execution. Has no effect if Executor is in
   * {@link com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State#NO_TASK_IN_PROGRESS} state.</li>
   * <li>Enable/disable self-healing for the specified anomaly types.</li>
   * <li>Drop selected recently removed/demoted brokers.</li>
   * <li>Enable/disable the specified concurrency adjusters.</li>
   * <li>Enable/disable the (At/Under)MinISR-based concurrency adjustment.</li>
   * </ul>
   *
   * @return Admin response.
   */
  @Override
  protected AdminResult handle() {
    // 1. Increase/decrease the specified proposal execution concurrency.
    String ongoingConcurrencyChangeRequest = processChangeExecutionConcurrencyRequest();

    // 2. Enable/disable self-healing for the specified anomaly types.
    Map<AnomalyType, Boolean> selfHealingBefore = new HashMap<>(KafkaAnomalyType.cachedValues().size());
    Map<AnomalyType, Boolean> selfHealingAfter = new HashMap<>(KafkaAnomalyType.cachedValues().size());
    processUpdateSelfHealingRequest(selfHealingBefore, selfHealingAfter);

    // 3. Drop selected recently removed/demoted brokers.
    String dropRecentBrokersRequest = processDropRecentBrokersRequest();

    // 4. Enable/disable (1) the specified concurrency adjusters and/or (2) the MinISR-based concurrency adjustment.
    Map<ConcurrencyType, Boolean> concurrencyAdjusterBefore = new HashMap<>(ConcurrencyType.cachedValues().size());
    Map<ConcurrencyType, Boolean> concurrencyAdjusterAfter = new HashMap<>(ConcurrencyType.cachedValues().size());
    StringBuilder minIsrBasedConcurrencyAdjustmentRequest = new StringBuilder();
    processUpdateConcurrencyAdjusterRequest(concurrencyAdjusterBefore, concurrencyAdjusterAfter, minIsrBasedConcurrencyAdjustmentRequest);

    return new AdminResult(selfHealingBefore,
                           selfHealingAfter,
                           ongoingConcurrencyChangeRequest,
                           dropRecentBrokersRequest,
                           concurrencyAdjusterBefore,
                           concurrencyAdjusterAfter,
                           minIsrBasedConcurrencyAdjustmentRequest.toString(),
                           _kafkaCruiseControl.config());
  }

  protected void processUpdateConcurrencyAdjusterRequest(Map<ConcurrencyType, Boolean> concurrencyAdjusterBefore,
                                                         Map<ConcurrencyType, Boolean> concurrencyAdjusterAfter,
                                                         StringBuilder minIsrBasedConcurrencyAdjustmentRequest) {
    UpdateConcurrencyAdjusterParameters updateConcurrencyAdjusterParameters = _parameters.updateConcurrencyAdjusterParameters();
    if (updateConcurrencyAdjusterParameters != null) {
      Set<ConcurrencyType> disableConcurrencyAdjusterFor = updateConcurrencyAdjusterParameters.disableConcurrencyAdjusterFor();
      Set<ConcurrencyType> enableConcurrencyAdjusterFor = updateConcurrencyAdjusterParameters.enableConcurrencyAdjusterFor();
      Boolean minIsrBasedConcurrencyAdjustment = updateConcurrencyAdjusterParameters.minIsrBasedConcurrencyAdjustment();
      if (minIsrBasedConcurrencyAdjustment != null) {
        boolean oldValue = _kafkaCruiseControl.setConcurrencyAdjusterMinIsrCheck(minIsrBasedConcurrencyAdjustment);
        minIsrBasedConcurrencyAdjustmentRequest.append(String.format("MinISR-based concurrency adjustment is modified (before: %s after: %s).",
                                                                     oldValue, minIsrBasedConcurrencyAdjustment));
        LOG.info("MinISR-based concurrency adjustment is modified by user (before: {} after: {}).", oldValue, minIsrBasedConcurrencyAdjustment);
      }

      for (ConcurrencyType type : disableConcurrencyAdjusterFor) {
        concurrencyAdjusterBefore.put(type, _kafkaCruiseControl.setConcurrencyAdjusterFor(type, false));
        concurrencyAdjusterAfter.put(type, false);
      }

      for (ConcurrencyType adjusterType : enableConcurrencyAdjusterFor) {
        concurrencyAdjusterBefore.put(adjusterType, _kafkaCruiseControl.setConcurrencyAdjusterFor(adjusterType, true));
        concurrencyAdjusterAfter.put(adjusterType, true);
      }

      if (!disableConcurrencyAdjusterFor.isEmpty() || !enableConcurrencyAdjusterFor.isEmpty()) {
        LOG.info("Concurrency adjuster state is modified by user (before: {} after: {}).", concurrencyAdjusterBefore, concurrencyAdjusterAfter);
      }
    }
  }

  protected String processChangeExecutionConcurrencyRequest() {
    ChangeExecutionConcurrencyParameters changeExecutionConcurrencyParameters = _parameters.changeExecutionConcurrencyParameters();
    if (changeExecutionConcurrencyParameters == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    // 1. Change inter-broker partition concurrency.
    Integer concurrentInterBrokerPartitionMovements = changeExecutionConcurrencyParameters.concurrentInterBrokerPartitionMovements();
    if (concurrentInterBrokerPartitionMovements != null) {
      _kafkaCruiseControl.setRequestedInterBrokerPartitionMovementConcurrency(concurrentInterBrokerPartitionMovements);
      sb.append(String.format("Inter-broker partition movement concurrency is set to %d%n", concurrentInterBrokerPartitionMovements));
      LOG.info("Inter-broker partition movement concurrency is set to: {} by user.", concurrentInterBrokerPartitionMovements);
    }
    // 2. Change intra-broker partition concurrency.
    Integer concurrentIntraBrokerPartitionMovements = changeExecutionConcurrencyParameters.concurrentIntraBrokerPartitionMovements();
    if (concurrentIntraBrokerPartitionMovements != null) {
      _kafkaCruiseControl.setRequestedIntraBrokerPartitionMovementConcurrency(concurrentIntraBrokerPartitionMovements);
      sb.append(String.format("Intra-broker partition movement concurrency is set to %d%n", concurrentIntraBrokerPartitionMovements));
      LOG.info("Intra-broker partition movement concurrency is set to: {} by user.", concurrentIntraBrokerPartitionMovements);
    }
    // 3. Change leadership concurrency.
    Integer concurrentLeaderMovements = changeExecutionConcurrencyParameters.concurrentLeaderMovements();
    if (concurrentLeaderMovements != null) {
      _kafkaCruiseControl.setRequestedLeadershipMovementConcurrency(concurrentLeaderMovements);
      sb.append(String.format("Leadership movement concurrency is set to %d%n", concurrentLeaderMovements));
      LOG.info("Leadership movement concurrency is set to: {} by user.", concurrentLeaderMovements);
    }
    // 4. Change the interval between checking and updating (if needed) the progress of an initiated execution.
    Long executionProgressCheckIntervalMs = changeExecutionConcurrencyParameters.executionProgressCheckIntervalMs();
    if (executionProgressCheckIntervalMs != null) {
      _kafkaCruiseControl.setRequestedExecutionProgressCheckIntervalMs(executionProgressCheckIntervalMs);
      sb.append(String.format("Execution progress check interval is set to %dMs%n", executionProgressCheckIntervalMs));
      LOG.info("Execution progress check interval is set to: {}Ms by user.", executionProgressCheckIntervalMs);
    }

    return sb.toString();
  }

  protected void processUpdateSelfHealingRequest(Map<AnomalyType, Boolean> selfHealingBefore, Map<AnomalyType, Boolean> selfHealingAfter) {
    UpdateSelfHealingParameters updateSelfHealingParameters = _parameters.updateSelfHealingParameters();
    if (updateSelfHealingParameters != null) {
      Set<AnomalyType> disableSelfHealingFor = updateSelfHealingParameters.disableSelfHealingFor();
      Set<AnomalyType> enableSelfHealingFor = updateSelfHealingParameters.enableSelfHealingFor();

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
    }
  }

  protected String processDropRecentBrokersRequest() {
    DropRecentBrokersParameters dropRecentBrokersParameters = _parameters.dropRecentBrokersParameters();
    if (dropRecentBrokersParameters == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    Set<Integer> brokersToDropFromRecentlyRemoved = dropRecentBrokersParameters.dropRecentlyRemovedBrokers();
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

    Set<Integer> brokersToDropFromRecentlyDemoted = dropRecentBrokersParameters.dropRecentlyDemotedBrokers();
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

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = _servlet.asyncKafkaCruiseControl();
    _parameters = (AdminParameters) validateNotNull(configs.get(ADMIN_PARAMETER_OBJECT_CONFIG),
                                                    "Parameter configuration is missing from the request.");
  }
}
