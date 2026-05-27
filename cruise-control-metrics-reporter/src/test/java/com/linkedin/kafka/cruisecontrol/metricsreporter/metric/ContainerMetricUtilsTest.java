/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class ContainerMetricUtilsTest {

  private static final double DELTA = 0.01;
  private static final double CPU_PERIOD = 100000.0;

  private void mockGetContainerProcessCpuLoad(int processors, double cpuQuota, double cpuUtil, double expectedLoad)
    throws Exception {
    // CALLS_REAL_METHODS makes getContainerProcessCpuLoad keep its real body; only the
    // three leaf static helpers below are replaced. Mockito-inline rewrites bytecode
    // for the scope of the try-with-resources block, so no PowerMock-style
    // classloader gymnastics and no JDK-internal API use.
    try (MockedStatic<ContainerMetricUtils> mocked =
             Mockito.mockStatic(ContainerMetricUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(ContainerMetricUtils::getAvailableProcessors).thenReturn(processors);
      mocked.when(ContainerMetricUtils::getCpuPeriod).thenReturn(CPU_PERIOD);
      mocked.when(ContainerMetricUtils::getCpuQuota).thenReturn(cpuQuota);

      assertEquals(expectedLoad, ContainerMetricUtils.getContainerProcessCpuLoad(cpuUtil), DELTA);
    }
  }

  @Test
  public void testGetContainerProcessCpuLoad() throws Exception {
    /*
     *  expectedContainerProcessCpuLoad = (cpuUtil * processors) / (cpuQuota / cpuPeriod)
     */
    mockGetContainerProcessCpuLoad(1, 100000.0, 1.0, 1.0);
    mockGetContainerProcessCpuLoad(1, 100000.0, 0.5, 0.5);
    mockGetContainerProcessCpuLoad(1, 50000.0, 0.5, 1.0);
    mockGetContainerProcessCpuLoad(1, 75000.0, 0.5, 0.66);

    mockGetContainerProcessCpuLoad(2, 100000.0, 0.5, 1.0);
    mockGetContainerProcessCpuLoad(2, 200000.0, 1.0, 1.0);
    mockGetContainerProcessCpuLoad(2, 25000.0, 0.125, 1.0);
    mockGetContainerProcessCpuLoad(2, 2500.0, 0.0125, 1.0);

    mockGetContainerProcessCpuLoad(2, ContainerMetricUtils.NO_CPU_QUOTA, 0.125, 0.125);
  }
}
