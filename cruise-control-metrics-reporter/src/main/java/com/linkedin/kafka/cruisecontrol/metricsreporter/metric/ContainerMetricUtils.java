/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ContainerMetricUtils {

    public double getCpuPeriod() {
        return Double.parseDouble(readFile(CgroupFiles.PERIOD_PATH.getValue()));
    }

    public double getCpuQuota() {
        return Double.parseDouble(readFile(CgroupFiles.QUOTA_PATH.getValue()));
    }

    /**
     * Gets the the number of logical cores available to the node.
     *
     * We can get this value while running in a container by using the "nproc" command.
     * Using other methods like OperatingSystemMXBean.getAvailableProcessors() and
     * Runtime.getRuntime().availableProcessors() would require disabling container
     * support (-XX:-UseContainerSupport) since these methods are aware of container
     * boundaries
     *
     * @return Number of logical processors on node
     */
    public int getAvailableProcessors() {
        int proc = 1;
        try {
            InputStream in = Runtime.getRuntime().exec("nproc").getInputStream();
            proc = Integer.parseInt(readStringValue(in));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return proc;
    }

    private static String readFile(String path) {
        String s = null;
        try {
            s = readStringValue(new FileInputStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return s;
    }

    private static String readStringValue(InputStream in) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = br.readLine()) != null) {
                return line;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Get the "recent CPU usage" for the JVM process running inside of a container.
     *
     * At this time, the methods of OperatingSystemMXBean used for retrieving recent CPU usage are not
     * container aware and calculate CPU usage with respect to the physical host instead of the operating
     * environment from which they are called from. There have been efforts to make these methods container
     * aware but the changes have not been backported to Java versions less than version 14.
     *
     * Once these changes get backported, https://bugs.openjdk.java.net/browse/JDK-8226575, we can use
     * "getSystemCpuLoad()" for retrieving the CPU usage values when running in a container environment.
     *
     * @param cpuUtil The "recent CPU usage" for a JVM process with respect to node
     * @return the "recent CPU usage" for a JVM process with respect to operating environment
     *         as a double in [0.0,1.0].
     */
    public double getContainerProcessCpuLoad(double cpuUtil) {
        int logicalProcessorsOfNode = getAvailableProcessors();
        double cpuQuota = getCpuQuota();
        if (cpuQuota == -1) {
            /* A CPU quota value of -1 indicates that the cgroup does not adhere to any CPU time restrictions so we
             * will use the original container agnostic CPU usage value.
             */
            return cpuUtil;
        }

        // Get the number of CPUs of a node that can be used by the operating environment
        double cpuLimit = (cpuQuota / getCpuPeriod());

        // Get the minimal number of CPUs needed to achieve the reported CPU utilization
        double cpus = cpuUtil * logicalProcessorsOfNode;

        /* Calculate the CPU utilization of a JVM process with respect to the operating environment.
         * Since the operating environment will only use the CPU resources allocated by CGroups,
         * it will always be that: cpuLimit >= cpus and the result is in the [0.0,1.0] interval.
         */
        return cpus / cpuLimit;
    }

    public enum CgroupFiles {

        QUOTA_PATH("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
        PERIOD_PATH("/sys/fs/cgroup/cpu/cpu.cfs_period_us");

        private String _value;

        CgroupFiles(String value) {
            this._value = value;
        }

        public String getValue() {
            return _value;
        }
    }
}
