/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddedOrRemovedBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.async.progress.Pending;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.http.HttpServletRequest;


/**
 * A KafkaCruiseControl extension with asynchronous operations support.
 *
 * The following async methods are supported:
 *
 * <ul>
 * <li>{@link KafkaCruiseControl#decommissionBrokers(Collection, boolean, boolean, List, ModelCompletenessRequirements,
 * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern, HttpServletRequest)}</li>
 * <li>{@link KafkaCruiseControl#addBrokers(Collection, boolean, boolean, List, ModelCompletenessRequirements,
 * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern, HttpServletRequest)}</li>
 * <li>{@link KafkaCruiseControl#demoteBrokers(Collection, boolean, OperationProgress, boolean, Integer, HttpServletRequest)}</li>
 * <li>{@link KafkaCruiseControl#clusterModel(long, ModelCompletenessRequirements, OperationProgress, boolean)}</li>
 * <li>{@link KafkaCruiseControl#clusterModel(long, long, Double, OperationProgress, boolean)}</li>
 * <li>{@link KafkaCruiseControl#getOptimizationProposals(OperationProgress, boolean)}</li>
 * <li>{@link KafkaCruiseControl#state(OperationProgress, Set, UserTaskManager)}</li>
 * <li>{@link KafkaCruiseControl#getOptimizationProposals(List, ModelCompletenessRequirements, OperationProgress,
 * boolean, boolean, java.util.regex.Pattern)}</li>
 * <li>{@link KafkaCruiseControl#fixOfflineReplicas(boolean, List, ModelCompletenessRequirements, OperationProgress,
 * boolean, Integer, Integer, boolean, java.util.regex.Pattern, HttpServletRequest)}</li>
 * <li>{@link KafkaCruiseControl#rebalance(List, boolean, ModelCompletenessRequirements, OperationProgress,
 * boolean, Integer, Integer, boolean, java.util.regex.Pattern, HttpServletRequest)}</li>
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
   * @see KafkaCruiseControl#state(OperationProgress, Set, UserTaskManager)
   */
  public OperationFuture state(CruiseControlStateParameters parameters,
                               UserTaskManager userTaskManager) {
    OperationFuture future = new OperationFuture("Get state");
    pending(future.operationProgress());
    _sessionExecutor.submit(new GetStateRunnable(this, future, parameters, userTaskManager));
    return future;
  }

  /**
   * @see KafkaCruiseControl#decommissionBrokers(Collection, boolean, boolean, List, ModelCompletenessRequirements,
   * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern, HttpServletRequest)
   */
  public OperationFuture decommissionBrokers(List<String> goals,
                                             ModelCompletenessRequirements requirements,
                                             AddedOrRemovedBrokerParameters parameters,
                                             HttpServletRequest request) {
    OperationFuture future = new OperationFuture("Decommission brokers");
    pending(future.operationProgress());
    _sessionExecutor.submit(new DecommissionBrokersRunnable(this, future, goals, requirements, parameters, request));
    return future;
  }

  /**
   * @see KafkaCruiseControl#fixOfflineReplicas(boolean, List, ModelCompletenessRequirements, OperationProgress,
   * boolean, Integer, Integer, boolean, java.util.regex.Pattern, HttpServletRequest)
   */
  public OperationFuture fixOfflineReplicas(List<String> goals,
                                            ModelCompletenessRequirements requirements,
                                            FixOfflineReplicasParameters parameters,
                                            HttpServletRequest request) {
    OperationFuture future = new OperationFuture("Fix offline replicas");
    pending(future.operationProgress());
    _sessionExecutor.submit(new FixOfflineReplicasRunnable(this, future, goals, requirements, parameters, request));

    return future;
  }

  /**
   * @see KafkaCruiseControl#addBrokers(Collection, boolean, boolean, List, ModelCompletenessRequirements,
   * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern, HttpServletRequest)
   */
  public OperationFuture addBrokers(List<String> goals,
                                    ModelCompletenessRequirements requirements,
                                    AddedOrRemovedBrokerParameters parameters,
                                    HttpServletRequest request) {
    OperationFuture future = new OperationFuture("Add brokers");
    pending(future.operationProgress());
    _sessionExecutor.submit(new AddBrokerRunnable(this, future, goals, requirements, parameters, request));
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
   * boolean, java.util.regex.Pattern)
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
   * Integer, boolean, java.util.regex.Pattern, HttpServletRequest)
   */
  public OperationFuture rebalance(List<String> goals,
                                   ModelCompletenessRequirements requirements,
                                   RebalanceParameters parameters,
                                   HttpServletRequest request) {
    OperationFuture future = new OperationFuture("Rebalance");
    pending(future.operationProgress());
    _sessionExecutor.submit(new RebalanceRunnable(this, future, goals, requirements, parameters, request));
    return future;
  }

  /**
   * @see KafkaCruiseControl#demoteBrokers(Collection, boolean, OperationProgress, boolean, Integer, HttpServletRequest)
   */
  public OperationFuture demoteBrokers(HttpServletRequest request, DemoteBrokerParameters parameters) {
    OperationFuture future = new OperationFuture("Demote");
    pending(future.operationProgress());
    _sessionExecutor.submit(new DemoteBrokerRunnable(this, future, request, parameters));
    return future;
  }

  private void pending(OperationProgress progress) {
    progress.addStep(new Pending());
  }
}
