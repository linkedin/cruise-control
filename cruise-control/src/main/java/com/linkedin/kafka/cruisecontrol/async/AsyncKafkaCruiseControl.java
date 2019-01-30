/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddedOrRemovedBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.async.progress.Pending;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * A KafkaCruiseControl extension with asynchronous operations support.
 *
 * The following async methods are supported:
 *
 * <ul>
 * <li>{@link KafkaCruiseControl#decommissionBrokers(java.util.Collection, boolean, boolean, List, ModelCompletenessRequirements,
 * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern, String, boolean, boolean)}</li>
 * <li>{@link KafkaCruiseControl#addBrokers(java.util.Collection, boolean, boolean, List, ModelCompletenessRequirements,
 * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern, String, boolean, boolean)}</li>
 * <li>{@link KafkaCruiseControl#demoteBrokers(java.util.Collection, boolean, OperationProgress, boolean, Integer, boolean,
 * boolean, String, boolean)}</li>
 * <li>{@link KafkaCruiseControl#clusterModel(long, ModelCompletenessRequirements, OperationProgress, boolean)}</li>
 * <li>{@link KafkaCruiseControl#clusterModel(long, long, Double, OperationProgress, boolean)}</li>
 * <li>{@link KafkaCruiseControl#getOptimizationProposals(OperationProgress, boolean)}</li>
 * <li>{@link KafkaCruiseControl#state(OperationProgress, Set)}</li>
 * <li>{@link KafkaCruiseControl#getOptimizationProposals(List, ModelCompletenessRequirements, OperationProgress,
 * boolean, boolean, java.util.regex.Pattern, boolean, boolean)}</li>
 * <li>{@link KafkaCruiseControl#rebalance(List, boolean, ModelCompletenessRequirements, OperationProgress,
 * boolean, Integer, Integer, boolean, java.util.regex.Pattern, String, boolean, boolean)}</li>
 * </ul>
 *
 * The other operations are non-blocking by default.
 */
public class AsyncKafkaCruiseControl extends KafkaCruiseControl {
  private final ExecutorService _sessionExecutor =
      Executors.newFixedThreadPool(3, new KafkaCruiseControlThreadFactory("ServletSessionExecutor",
                                                                          true,
                                                                          null));

  /**
   * Construct the Cruise Control
   *
   * @param config the configuration of Cruise Control.
   * @param dropwizardMetricRegistry the metric registry for metrics.
   */
  public AsyncKafkaCruiseControl(KafkaCruiseControlConfig config, MetricRegistry dropwizardMetricRegistry) {
    super(config, dropwizardMetricRegistry);
  }

  /**
   * @see KafkaCruiseControl#state(OperationProgress, Set)
   */
  public OperationFuture state(CruiseControlStateParameters parameters) {
    OperationFuture future = new OperationFuture("Get state");
    pending(future.operationProgress());
    _sessionExecutor.submit(new GetStateRunnable(this, future, parameters));
    return future;
  }

  /**
   * @see KafkaCruiseControl#decommissionBrokers(java.util.Collection, boolean, boolean, List, ModelCompletenessRequirements,
   * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern, String, boolean, boolean)
   */
  public OperationFuture decommissionBrokers(List<String> goals,
                                             ModelCompletenessRequirements requirements,
                                             AddedOrRemovedBrokerParameters parameters,
                                             String uuid) {
    OperationFuture future = new OperationFuture("Decommission brokers");
    pending(future.operationProgress());
    _sessionExecutor.submit(new DecommissionBrokersRunnable(this, future, goals, requirements, parameters, uuid));
    return future;
  }

  /**
   * @see KafkaCruiseControl#addBrokers(java.util.Collection, boolean, boolean, List, ModelCompletenessRequirements,
   * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern, String, boolean, boolean)
   */
  public OperationFuture addBrokers(List<String> goals,
                                    ModelCompletenessRequirements requirements,
                                    AddedOrRemovedBrokerParameters parameters,
                                    String uuid) {
    OperationFuture future = new OperationFuture("Add brokers");
    pending(future.operationProgress());
    _sessionExecutor.submit(new AddBrokerRunnable(this, future, goals, requirements, parameters, uuid));
    return future;
  }

  /**
   * @see KafkaCruiseControl#clusterModel(long, long, Double, OperationProgress, boolean)
   */
  public OperationFuture partitionLoadState(PartitionLoadParameters parameters) {
    OperationFuture future =
        new OperationFuture(String.format("Get partition load from %d to %d", parameters.startMs(), parameters.endMs()));
    pending(future.operationProgress());
    _sessionExecutor.submit(new GetClusterModelInRangeRunnable(this, future, parameters));
    return future;
  }

  /**
   * @see KafkaCruiseControl#clusterModel(long, ModelCompletenessRequirements, OperationProgress, boolean)
   */
  public OperationFuture getBrokerStats(ClusterLoadParameters parameters) {
    OperationFuture future = new OperationFuture("Get broker stats");
    pending(future.operationProgress());
    _sessionExecutor.submit(new GetBrokerStatsRunnable(this, future, parameters));
    return future;
  }

  /**
   * @see KafkaCruiseControl#getOptimizationProposals(List, ModelCompletenessRequirements, OperationProgress, boolean,
   * boolean, java.util.regex.Pattern, boolean, boolean)
   */
  public OperationFuture getOptimizationProposals(List<String> goals,
                                                  ModelCompletenessRequirements requirements,
                                                  ProposalsParameters parameters) {
    OperationFuture future = new OperationFuture("Get customized optimization proposals");
    pending(future.operationProgress());
    _sessionExecutor.submit(new GetOptimizationProposalsRunnable(this, future, goals, requirements, parameters));
    return future;
  }

  /**
   * @see KafkaCruiseControl#rebalance(List, boolean, ModelCompletenessRequirements, OperationProgress, boolean, Integer,
   * Integer, boolean, java.util.regex.Pattern, String, boolean, boolean)
   */
  public OperationFuture rebalance(List<String> goals,
                                   ModelCompletenessRequirements requirements,
                                   RebalanceParameters parameters,
                                   String uuid) {
    OperationFuture future = new OperationFuture("Rebalance");
    pending(future.operationProgress());
    _sessionExecutor.submit(new RebalanceRunnable(this, future, goals, requirements, parameters, uuid));
    return future;
  }

  /**
   * @see KafkaCruiseControl#demoteBrokers(java.util.Collection, boolean, OperationProgress, boolean, Integer, boolean,
   * boolean, String, boolean)
   */
  public OperationFuture demoteBrokers(String uuid, DemoteBrokerParameters parameters) {
    OperationFuture future = new OperationFuture("Demote");
    pending(future.operationProgress());
    _sessionExecutor.submit(new DemoteBrokerRunnable(this, future, uuid, parameters));
    return future;
  }

  private void pending(OperationProgress progress) {
    progress.addStep(new Pending());
  }
}
