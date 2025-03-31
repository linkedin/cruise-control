package com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class VolumeInfo {
    private String volumeId;
    private String volumeType;
    private int iops;
    private int size;

    public VolumeInfo() {
    }

    public VolumeInfo(String volumeId,
                      String volumeType,
                      int iops,
                      int size) {
        this.volumeId = volumeId;
        this.volumeType = volumeType;
        this.iops = iops;
        this.size = size;
    }

    public String getVolumeId() {
        return volumeId;
    }

    public String getVolumeType() {
        return volumeType;
    }

    public int getIops() {
        return iops;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VolumeInfo{");
        sb.append("volumeId='").append(volumeId).append('\'');
        sb.append(", volumeType='").append(volumeType).append('\'');
        sb.append(", iops=").append(iops);
        sb.append(", size=").append(size);
        sb.append('}');
        return sb.toString();
    }
}
