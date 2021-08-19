/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.model.RawAndDerivedResource;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;


public final class OfflineProposalGenerator {

  private OfflineProposalGenerator() {

  }

  /**
   * The main function to run offline proposal generator.
   * @param argv Arguments passed while starting offline proposal generator.
   */
  public static void main(String[] argv) throws Exception {
    //TODO: probably need to save this in the original model file
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    ModelUtils.init(config);
    ModelParameters.init(config);
    BalancingConstraint balancingConstraint = new BalancingConstraint(config);

    long start = System.currentTimeMillis();
    ClusterModel clusterModel = clusterModelFromFile(argv[0]);
    long end = System.currentTimeMillis();
    double duration = (end - start) / 1000.0;
    System.out.println("Model loaded in " + duration + "s.");

    ClusterModelStats origStats = clusterModel.getClusterStats(balancingConstraint);

    String loadBeforeOptimization = clusterModel.brokerStats(null).toString();
    // Instantiate the components.
    GoalOptimizer goalOptimizer = new GoalOptimizer(config,
                                                    null,
                                                    new SystemTime(),
                                                    new MetricRegistry(),
                                                    EasyMock.mock(Executor.class),
                                                    EasyMock.mock(AdminClient.class));
    start = System.currentTimeMillis();
    OptimizerResult optimizerResult = goalOptimizer.optimizations(clusterModel, new OperationProgress());
    end = System.currentTimeMillis();
    duration = (end - start) / 1000.0;
    String loadAfterOptimization = clusterModel.brokerStats(null).toString();
    System.out.println("Optimize goals in " + duration + "s.");
    System.out.println(optimizerResult.goalProposals().size());
    System.out.println(loadBeforeOptimization);
    System.out.println(loadAfterOptimization);

    ClusterModelStats optimizedStats = clusterModel.getClusterStats(balancingConstraint);

    double[] testStatistics = AnalyzerUtils.testDifference(origStats.utilizationMatrix(), optimizedStats.utilizationMatrix());
    System.out.println(Arrays.stream(RawAndDerivedResource.values()).map(Enum::toString).collect(Collectors.joining(", ")));
    System.out.println(Arrays.stream(testStatistics).boxed().map(pValue -> Double.toString(pValue)).collect(Collectors.joining(", ")));

  }

  private static ClusterModel clusterModelFromFile(String name)
    throws IOException, ClassNotFoundException {
    try (FileInputStream fin = new FileInputStream(name);
      BufferedInputStream bin = new BufferedInputStream(fin);
      ObjectInputStream oin = new ObjectInputStream(bin)) {
      return (ClusterModel) oin.readObject();
    }
  }
}
