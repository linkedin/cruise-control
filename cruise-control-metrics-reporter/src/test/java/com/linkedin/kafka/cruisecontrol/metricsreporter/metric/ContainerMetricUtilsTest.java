/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import org.easymock.EasyMock;
import org.junit.Test;

import static org.easymock.EasyMock.partialMockBuilder;
import static org.junit.Assert.assertEquals;

public class ContainerMetricUtilsTest {

    private static final double DELTA = 0.01;

    private void mockGetContainerProcessCpuLoad(int processors, double cpuQuota, double cpuPeriod, double cpuUtil, double expectedLoad) {
        ContainerMetricUtils cmu = partialMockBuilder(ContainerMetricUtils.class)
                .addMockedMethod("getAvailableProcessors")
                .addMockedMethod("getCpuPeriod")
                .addMockedMethod("getCpuQuota")
                .createMock();

        EasyMock.expect(cmu.getAvailableProcessors()).andReturn(processors);
        EasyMock.expect(cmu.getCpuPeriod()).andReturn(cpuPeriod);
        EasyMock.expect(cmu.getCpuQuota()).andReturn(cpuQuota);

        EasyMock.replay(cmu);

        assertEquals(expectedLoad, cmu.getContainerProcessCpuLoad(cpuUtil), DELTA);
    }

    @Test
    public void testGetContainerProcessCpuLoad() {
        /*
         *  expectedContainerProcessCpuLoad = (cpuUtil * processors) / (cpuQuota / cpuPeriod)
         */
        mockGetContainerProcessCpuLoad(1, 100000.0, 100000.0, 1.0, 1.0);
        mockGetContainerProcessCpuLoad(1, 100000.0, 100000.0, 0.5, 0.5);
        mockGetContainerProcessCpuLoad(1, 50000.0, 100000.0, 0.5, 1.0);
        mockGetContainerProcessCpuLoad(1, 75000.0, 100000.0, 0.5, 0.66);

        mockGetContainerProcessCpuLoad(2, 100000.0, 100000.0, 0.5, 1.0);
        mockGetContainerProcessCpuLoad(2, 200000.0, 100000.0, 1.0, 1.0);
        mockGetContainerProcessCpuLoad(2, 25000.0, 100000.0, 0.125, 1.0);
        mockGetContainerProcessCpuLoad(2, 2500.0, 100000.0, 0.0125, 1.0);

        mockGetContainerProcessCpuLoad(2, -1.0, 100000.0, 0.125, 0.125);
    }
}
