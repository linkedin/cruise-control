/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ContainerMetricUtils.class)
public class ContainerMetricUtilsTest {

  private static final double DELTA = 0.01;
  private static final double CPU_PERIOD = 100000.0;

  private void mockGetContainerProcessCpuLoad(int processors, double cpuQuota, double cpuUtil, double expectedLoad)
    throws Exception {
    PowerMock.mockStaticPartial(ContainerMetricUtils.class,
      "getAvailableProcessors",
      "getCpuPeriod",
      "getCpuQuota");

    PowerMock.expectPrivate(ContainerMetricUtils.class, "getAvailableProcessors").andReturn(processors);
    PowerMock.expectPrivate(ContainerMetricUtils.class, "getCpuPeriod").andReturn(CPU_PERIOD);
    PowerMock.expectPrivate(ContainerMetricUtils.class, "getCpuQuota").andReturn(cpuQuota);
    PowerMock.expectPrivate(ContainerMetricUtils.class, "getCpuQuota").andReturn(cpuQuota);

    PowerMock.replay(ContainerMetricUtils.class);

    assertEquals(expectedLoad, ContainerMetricUtils.getContainerProcessCpuLoad(cpuUtil), DELTA);
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
