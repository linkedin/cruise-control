package com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class CpuOptions {
    private int coreCount;
    private int threadsPerCore;

    public CpuOptions() { }

    public CpuOptions(int coreCount, int threadsPerCore) {
        this.coreCount = coreCount;
        this.threadsPerCore = threadsPerCore;
    }

    public int getCoreCount() {
        return coreCount;
    }

    public int getThreadsPerCore() {
        return threadsPerCore;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CpuOptions{");
        sb.append("coreCount=").append(coreCount);
        sb.append(", threadsPerCore=").append(threadsPerCore);
        sb.append('}');
        return sb.toString();
    }
}
