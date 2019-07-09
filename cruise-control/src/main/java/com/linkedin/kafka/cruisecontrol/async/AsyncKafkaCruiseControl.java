/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
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
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;


/**
 * A KafkaCruiseControl extension with asynchronous operations support.
 *
 * The following async methods are supported:
 *
 * <ul>
 * <li>{@link KafkaCruiseControl#decommissionBrokers(Set, boolean, boolean, java.util.List, ModelCompletenessRequirements,
 * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern,
 * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean, boolean, Set)}</li>
 * <li>{@link KafkaCruiseControl#addBrokers(Set, boolean, boolean, java.util.List, ModelCompletenessRequirements,
 * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern,
 * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean, boolean)}</li>
 * <li>{@link KafkaCruiseControl#demoteBrokers(Set, java.util.Map, boolean, OperationProgress, boolean, Integer, boolean, boolean,
 * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean)}</li>
 * <li>{@link KafkaCruiseControl#clusterModel(long, ModelCompletenessRequirements, OperationProgress, boolean, boolean)}</li>
 * <li>{@link KafkaCruiseControl#clusterModel(long, long, Double, OperationProgress, boolean)}</li>
 * <li>{@link KafkaCruiseControl#getProposals(OperationProgress, boolean)}</li>
 * <li>{@link KafkaCruiseControl#state(OperationProgress, Set)}</li>
 * <li>{@link KafkaCruiseControl#getProposals(java.util.List, ModelCompletenessRequirements, OperationProgress,
 * boolean, boolean, java.util.regex.Pattern, boolean, boolean, boolean, boolean, Set, boolean)}</li>
 * <li>{@link KafkaCruiseControl#fixOfflineReplicas(boolean, java.util.List, ModelCompletenessRequirements, OperationProgress,
 * boolean, Integer, Integer, boolean, java.util.regex.Pattern,
 * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean, boolean)}</li>
 * <li>{@link KafkaCruiseControl#rebalance(java.util.List, boolean, ModelCompletenessRequirements, OperationProgress,
 * boolean, Integer, Integer, Integer, boolean, java.util.regex.Pattern,
 * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean, boolean, boolean,
 * boolean, Set, boolean)}</li>
 * <li>{@link KafkaCruiseControl#updateTopicConfiguration(java.util.regex.Pattern, java.util.List, short, boolean,
 * ModelCompletenessRequirements, OperationProgress, boolean, Integer, Integer, boolean,
 * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, boolean, boolean, boolean, String)}</li>
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
   * @see KafkaCruiseControl#decommissionBrokers(Set, boolean, boolean, java.util.List,
   * ModelCompletenessRequirements, OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern,
   * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean, boolean, Set)
   */
  public OperationFuture decommissionBrokers(RemoveBrokerParameters parameters, String uuid) {
    OperationFuture future = new OperationFuture("Decommission brokers");
    pending(future.operationProgress());
    _sessionExecutor.submit(new DecommissionBrokersRunnable(this, future, parameters, uuid, _config));
    return future;
  }

  /**
   * @see KafkaCruiseControl#fixOfflineReplicas(boolean, java.util.List, ModelCompletenessRequirements, OperationProgress,
   * boolean, Integer, Integer, boolean, java.util.regex.Pattern,
   * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean, boolean)
   */
  public OperationFuture fixOfflineReplicas(FixOfflineReplicasParameters parameters, String uuid) {
    OperationFuture future = new OperationFuture("Fix offline replicas");
    pending(future.operationProgress());
    _sessionExecutor.submit(new FixOfflineReplicasRunnable(this, future, parameters, uuid, _config));

    return future;
  }

  /**
   * @see KafkaCruiseControl#addBrokers(Set, boolean, boolean, java.util.List, ModelCompletenessRequirements,
   * OperationProgress, boolean, Integer, Integer, boolean, java.util.regex.Pattern,
   * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean, boolean)
   */
  public OperationFuture addBrokers(AddBrokerParameters parameters, String uuid) {
    OperationFuture future = new OperationFuture("Add brokers");
    pending(future.operationProgress());
    _sessionExecutor.submit(new AddBrokerRunnable(this, future, parameters, uuid, _config));
    return future;
  }

  /**
   * @see KafkaCruiseControl#clusterModel(long, long, Double, OperationProgress, boolean)
   */
  public OperationFuture partitionLoadState(PartitionLoadParameters parameters) {
    OperationFuture future =
        new OperationFuture(String.format("Get partition load from %d to %d", parameters.startMs(), parameters.endMs()));
    pending(future.operationProgress());
    _sessionExecutor.submit(new GetClusterModelInRangeRunnable(this, future, parameters, _config));
    return future;
  }

  /**
   * @see KafkaCruiseControl#clusterModel(long, ModelCompletenessRequirements, OperationProgress, boolean, boolean)
   */
  public OperationFuture getBrokerStats(ClusterLoadParameters parameters) {
    OperationFuture future = new OperationFuture("Get broker stats");
    pending(future.operationProgress());
    _sessionExecutor.submit(new GetBrokerStatsRunnable(this, future, parameters, _config));
    return future;
  }

  /**
   * @see KafkaCruiseControl#getProposals(java.util.List, ModelCompletenessRequirements, OperationProgress,
   * boolean, boolean, java.util.regex.Pattern, boolean, boolean, boolean, boolean, Set, boolean)
   */
  public OperationFuture getProposals(ProposalsParameters parameters) {
    OperationFuture future = new OperationFuture("Get customized proposals");
    pending(future.operationProgress());
    _sessionExecutor.submit(new GetProposalsRunnable(this, future, parameters, _config));
    return future;
  }

  /**
   * @see KafkaCruiseControl#rebalance(java.util.List, boolean, ModelCompletenessRequirements, OperationProgress,
   * boolean, Integer, Integer, Integer, boolean, java.util.regex.Pattern,
   * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean, boolean,
   * boolean, boolean, Set, boolean)
   */
  public OperationFuture rebalance(RebalanceParameters parameters, String uuid) {
    OperationFuture future = new OperationFuture("Rebalance");
    pending(future.operationProgress());
    _sessionExecutor.submit(new RebalanceRunnable(this, future, parameters, uuid, _config));
    return future;
  }

  /**
   * @see KafkaCruiseControl#demoteBrokers(Set, java.util.Map, boolean, OperationProgress, boolean, Integer, boolean, boolean,
   * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, String, boolean)
   */
  public OperationFuture demoteBrokers(String uuid, DemoteBrokerParameters parameters) {
    OperationFuture future = new OperationFuture("Demote");
    pending(future.operationProgress());
    _sessionExecutor.submit(new DemoteBrokerRunnable(this, future, uuid, parameters, _config));
    return future;
  }

  /**
   * @see KafkaCruiseControl#updateTopicConfiguration(Pattern, java.util.List, short, boolean, ModelCompletenessRequirements,
   * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, Integer, Integer, boolean,
   * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy, Long, boolean, boolean, boolean, String)}
   */
  public OperationFuture updateTopicConfiguration(TopicConfigurationParameters parameters, String uuid) {
    OperationFuture future = new OperationFuture("UpdateTopicConfiguration");
    pending(future.operationProgress());
    _sessionExecutor.submit(new UpdateTopicConfigurationRunnable(this, future, uuid, parameters, _config));
    return future;
  }

  private void pending(OperationProgress progress) {
    progress.addStep(new Pending());
  }
}
