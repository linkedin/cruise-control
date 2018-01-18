/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class KafkaAssignerOptimizationVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAssignerOptimizationVerifier.class);

  private KafkaAssignerOptimizationVerifier() {
  }

  static GoalOptimizer.OptimizerResult executeGoalsFor(ClusterModel clusterModel,
                                                       Map<Integer, String> goalNameByPriority) throws Exception {
    return executeGoalsFor(clusterModel, goalNameByPriority, Collections.emptySet());
  }

  static GoalOptimizer.OptimizerResult executeGoalsFor(ClusterModel clusterModel,
                                                       Map<Integer, String> goalNameByPriority,
                                                       Collection<String> excludedTopics)
      throws Exception {
    // Set goals by their priority.
    SortedMap<Integer, Goal> goalByPriority = new TreeMap<>();
    for (Map.Entry<Integer, String> goalEntry : goalNameByPriority.entrySet()) {
      Integer priority = goalEntry.getKey();
      String goalClassName = goalEntry.getValue();

      Class<? extends Goal> goalClass = (Class<? extends Goal>) Class.forName(goalClassName);
      goalByPriority.put(priority, goalClass.newInstance());
    }

    // Generate the goalOptimizer and optimize given goals.
    long startTime = System.currentTimeMillis();
    Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
    StringJoiner stringJoiner = new StringJoiner(",");
    excludedTopics.forEach(stringJoiner::add);
    props.setProperty(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG, stringJoiner.toString());
    GoalOptimizer goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(props),
                                                    null,
                                                    new SystemTime(),
                                                    new MetricRegistry());
    GoalOptimizer.OptimizerResult optimizerResult = goalOptimizer.optimizations(clusterModel, goalByPriority);
    LOG.trace("Took {} ms to execute {} to generate {} proposals.", System.currentTimeMillis() - startTime,
              goalByPriority, optimizerResult.goalProposals().size());

    return optimizerResult;
  }
}
