/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;
import java.util.regex.Pattern;


/**
 * The async runnable for {@link KafkaCruiseControl#getProposals(
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean)} and
 * {@link KafkaCruiseControl#getProposals(List, ModelCompletenessRequirements,
 * com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress, boolean, boolean, Pattern, boolean, boolean, boolean)}
 */
class GetProposalsRunnable extends OperationRunnable {
  private final List<String> _goals;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Pattern _excludedTopics;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final boolean _ignoreProposalCache;

  GetProposalsRunnable(KafkaCruiseControl kafkaCruiseControl,
                       OperationFuture future,
                       ProposalsParameters parameters) {
    super(kafkaCruiseControl, future);
    _goals = parameters.goals();
    _modelCompletenessRequirements = parameters.modelCompletenessRequirements();
    _allowCapacityEstimation = parameters.allowCapacityEstimation();
    _excludedTopics = parameters.excludedTopics();
    _excludeRecentlyDemotedBrokers = parameters.excludeRecentlyDemotedBrokers();
    _excludeRecentlyRemovedBrokers = parameters.excludeRecentlyRemovedBrokers();
    _ignoreProposalCache = parameters.ignoreProposalCache();
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(_kafkaCruiseControl.getProposals(_goals,
                                                                   _modelCompletenessRequirements,
                                                                   _future.operationProgress(),
                                                                   _allowCapacityEstimation,
                                                                   true,
                                                                   _excludedTopics,
                                                                   _excludeRecentlyDemotedBrokers,
                                                                   _excludeRecentlyRemovedBrokers,
                                                                   _ignoreProposalCache));
  }
}
