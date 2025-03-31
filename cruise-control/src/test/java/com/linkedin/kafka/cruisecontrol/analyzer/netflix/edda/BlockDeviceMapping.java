package com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class BlockDeviceMapping {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Ebs {
        private String status;
        private String volumeId;

        public Ebs() { }
        public Ebs(String status, String volumeId) {
            this.status = status;
            this.volumeId = volumeId;
        }

        public String getStatus() {
            return status;
        }

        public String getVolumeId() {
            return volumeId;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Ebs{");
            sb.append("status='").append(status).append('\'');
            sb.append(", volumeId='").append(volumeId).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    private Ebs ebs;
    private String deviceName;

    public BlockDeviceMapping() { }

    public BlockDeviceMapping(Ebs ebs, String deviceName) {
        this.ebs = ebs;
        this.deviceName = deviceName;
    }

    public Ebs getEbs() {
        return ebs;
    }

    public String getDeviceName() {
        return deviceName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BlockDeviceMapping{");
        sb.append("ebs=").append(ebs);
        sb.append(", deviceName='").append(deviceName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
