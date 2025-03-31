package com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceInfo {
    private String privateIpAddress;
    private String instanceId;
    private CpuOptions cpuOptions;
    private List<BlockDeviceMapping> blockDeviceMappings;
    private String instanceType;

    public InstanceInfo() {
    }

    public InstanceInfo(String privateIpAddress,
                        String instanceId,
                        CpuOptions cpuOptions,
                        String instanceType,
                        List<BlockDeviceMapping> blockDeviceMappings) {
        this.privateIpAddress = privateIpAddress;
        this.instanceId = instanceId;
        this.cpuOptions = cpuOptions;
        this.instanceType = instanceType;
        this.blockDeviceMappings = blockDeviceMappings;
    }

    public String getPrivateIpAddress() {
        return privateIpAddress;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public CpuOptions getCpuOptions() {
        return cpuOptions;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public List<BlockDeviceMapping> getBlockDeviceMappings() {
        return blockDeviceMappings;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InstanceInfo{");
        sb.append("privateIpAddress='").append(privateIpAddress).append('\'');
        sb.append(", instanceId='").append(instanceId).append('\'');
        sb.append(", cpuOptions=").append(cpuOptions);
        sb.append(", blockDeviceMappings=").append(blockDeviceMappings);
        sb.append(", instanceType='").append(instanceType).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
