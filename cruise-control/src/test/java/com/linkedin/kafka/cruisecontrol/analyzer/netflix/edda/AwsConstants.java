/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsConstants {

    private static final Logger logger = LoggerFactory.getLogger(AwsConstants.class);
    // Network limit in KBps (ref: https://confluence.netflix.net/display/CLDPERF/AWS+Instances+Network+Throughput)
    private static final Map<InstanceType, Double> NETWORK_LIMIT_KBPS = new HashMap<>();

    private AwsConstants() {
    }

    public enum AvailabilityZone {
        US_EAST_1C("us-east-1c"),
        US_EAST_1D("us-east-1d"),
        US_EAST_1E("us-east-1e"),
        US_WEST_2A("us-west-2a"),
        US_WEST_2B("us-west-2b"),
        US_WEST_2C("us-west-2c"),
        EU_WEST_1A("eu-west-1a"),
        EU_WEST_1B("eu-west-1b"),
        EU_WEST_1C("eu-west-1c");

        private static final Map<String, AvailabilityZone> LOOKUP = new HashMap<>();
        static {
            for (AvailabilityZone zone : AvailabilityZone.values()) {
                LOOKUP.put(zone.getName(), zone);
            }
        }

        private final String name;
        AvailabilityZone(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }

        //This method can be used for reverse lookup purpose
        public static AvailabilityZone get(String zone) {
            return LOOKUP.get(zone.toLowerCase());
        }
    }

    public enum Region {
        useast1("us-east-1"),
        uswest2("us-west-2"),
        euwest1("eu-west-1"),
        useast2("us-east-2");

        private static final Map<String, Region> LOOKUP = new HashMap<>();
        static {
            for (Region region : Region.values()) {
                LOOKUP.put(region.getName(), region);
                LOOKUP.put(region.name(), region);
            }
        }

        private final String name;

        Region(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        //This method can be used for reverse lookup purpose
        public static Region get(String region) {
            return LOOKUP.get(region.toLowerCase());
        }
    }

    public enum Account {
        test, prod
    }

    public enum InstanceType {
        M3_L("m3.large"),
        M4_L("m4.large"),
        M5_L("m5.large"),
        M3_XL("m3.xlarge"),
        M5_XL("m5.xlarge"),
        R4_2XL("r4.2xlarge"),
        R5_XL("r5.xlarge"),
        R5_2XL("r5.2xlarge"),
        R5_4XL("r5.4xlarge"),
        R5_8XL("r5.8xlarge"),
        D2_XL("d2.xlarge"),
        D2_2XL("d2.2xlarge"),
        D2_4XL("d2.4xlarge"),
        D2_8XL("d2.8xlarge"),
        I3_2XL("i3.2xlarge"),
        I3_4XL("i3.4xlarge"),
        I3EN_XL("i3en.xlarge"),
        I3EN_2XL("i3en.2xlarge"),
        I3EN_3XL("i3en.3xlarge"),
        I4I_XL("i4i.xlarge"),
        I4I_2XL("i4i.2xlarge"),
        I4I_4XL("i4i.4xlarge");

        //Reverse Lookup table
        private static final Map<String, InstanceType> LOOKUP = new HashMap<>();
        static {
            for (InstanceType type : InstanceType.values()) {
                LOOKUP.put(type.getName(), type);
            }
        }

        private final String name;

        InstanceType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        //This method can be used for reverse lookup purpose
        public static InstanceType get(String instanceType) {
            InstanceType result = LOOKUP.get(instanceType.toLowerCase());
            if (result == null) {
                logger.error("Failed to lookup instanceType {}", instanceType);
            }
            return result;
        }
    }

    static {
        NETWORK_LIMIT_KBPS.put(InstanceType.M3_L, 87_500.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.M3_XL, 87_500.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.M4_L, 87_500.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.M5_L, 87_500.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.M5_XL, 125_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.R4_2XL, 250_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.R5_XL, 125_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.R5_2XL, 250_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.R5_4XL, 500_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.R5_8XL, 500_000.0); 
        NETWORK_LIMIT_KBPS.put(InstanceType.D2_XL, 87_500.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.D2_2XL, 250_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.D2_4XL, 500_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.D2_8XL, 1_175_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.I3_2XL, 250_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.I3_4XL, 500_000.0);
        NETWORK_LIMIT_KBPS.put(InstanceType.I3EN_XL, 1_175_000.0); 
        NETWORK_LIMIT_KBPS.put(InstanceType.I3EN_2XL, 1_175_000.0); 
        NETWORK_LIMIT_KBPS.put(InstanceType.I3EN_3XL, 1_175_000.0); 
        NETWORK_LIMIT_KBPS.put(InstanceType.I4I_XL, 125_000.0); 
        NETWORK_LIMIT_KBPS.put(InstanceType.I4I_2XL, 250_000.0); 
        NETWORK_LIMIT_KBPS.put(InstanceType.I4I_4XL, 500_000.0); 
    }

    public static Double getNetworkLimit(InstanceType instanceType) {
        return NETWORK_LIMIT_KBPS.getOrDefault(instanceType, NETWORK_LIMIT_KBPS.get(InstanceType.R5_8XL));
    }
}
