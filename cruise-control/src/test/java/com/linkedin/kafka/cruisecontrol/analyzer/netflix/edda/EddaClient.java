/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.AwsConstants.InstanceType.I3EN_2XL;
import static com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.AwsConstants.InstanceType.I3EN_3XL;
import static com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.AwsConstants.InstanceType.I3_2XL;
import static com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.AwsConstants.InstanceType.I3_4XL;
import static com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.AwsConstants.InstanceType.I4I_2XL;
import static com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.AwsConstants.InstanceType.I4I_4XL;
import static com.linkedin.kafka.cruisecontrol.analyzer.netflix.edda.AwsConstants.InstanceType.I4I_XL;

public final class EddaClient {
    private static final Logger logger = LoggerFactory.getLogger(EddaClient.class);
    // EDDA Url format: http://edda-${STACK}.${REGION}.${ENVIRONMENT}.netflix.net
    public static final String EDDA_URL_FMT = "http://edda-main.%s.%s.netflix.net";
    public static final String EDDA_INSTANCES_URL_FMT = EDDA_URL_FMT + "/api/v2/view/instances";
    public static final String EDDA_VOLUMES_URL_FMT = EDDA_URL_FMT + "/api/v2/aws/volumes";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final TypeReference<InstanceInfo> INSTANCE_INFO_TYPEREF =
            new TypeReference<InstanceInfo>() { };
    private static final TypeReference<List<VolumeInfo>> VOLUME_INFO_TYPEREF =
            new TypeReference<List<VolumeInfo>>() { };

    // 1000 GB
    private static final int DEFAULT_DISK_SIZE_MB = 1000_000;
    // Disk size for instance type in MB
    // https://ec2types.test.netflix.net/ec2?types=all
    private static final Map<AwsConstants.InstanceType, Integer> INSTANCE_TYPE_TO_DISK_SIZE_MAP = Map.ofEntries(
            Map.entry(I3_4XL, 3_800_000),
            Map.entry(I3_2XL, 1_900_000),
            Map.entry(I3EN_2XL, 5_000_000),
            Map.entry(I3EN_3XL, 7_500_000),
            Map.entry(I4I_XL, 937_000),
            Map.entry(I4I_2XL, 1_875_000),
            Map.entry(I4I_4XL, 3_750_000)
    );

    private EddaClient() { }

    public static InstanceInfo getInstanceInfo(String host, AwsConstants.Account account,
                                               AwsConstants.Region region)
            throws IOException {
        StringBuilder sb = new StringBuilder(String.format(EDDA_INSTANCES_URL_FMT, region.getName(), account.name()));
        if (host.startsWith("i-")) {
            sb = sb.append("/");
        } else if (host.startsWith("ec2")) {
            sb = sb.append(";publicDnsName=");
        } else {
            sb = sb.append(";privateIpAddress=");
        }
        final String uri = sb
                .append(host)
                .append(";")
                .append("_expand:(privateIpAddress,instanceId,cpuOptions,instanceType,blockDeviceMappings)")
                .toString();
        logger.debug("getting instance info (uri: {})", uri);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                HttpEntity entity = response.getEntity();
                String instanceInfo = EntityUtils.toString(entity);
                return OBJECT_MAPPER.readValue(instanceInfo, INSTANCE_INFO_TYPEREF);
            }
        }
    }

    public static VolumeInfo getVolumeInfo(String volumeId,
                                           AwsConstants.Account account,
                                           AwsConstants.Region region)
            throws IOException {
        final String uri = new StringBuilder(String.format(EDDA_VOLUMES_URL_FMT, region.getName(), account.name()))
                .append(";")
                .append("volumeId=")
                .append(volumeId)
                .append(";")
                .append("_expand:(volumeId,volumeType,iops,size)")
                .toString();

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                HttpEntity entity = response.getEntity();
                String volumeInfo = EntityUtils.toString(entity);
                List<VolumeInfo> volumeInfos = OBJECT_MAPPER.readValue(volumeInfo, VOLUME_INFO_TYPEREF);
                if (volumeInfos.size() > 0) {
                    return volumeInfos.get(0);
                } else {
                    throw new NoSuchElementException("did not find volume info in Edda");
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        try {
            InstanceInfo instanceInfo = EddaClient.getInstanceInfo("ec2-18-237-75-15.us-west-2.compute.amazonaws.com", AwsConstants.Account.test,
                    AwsConstants.Region.uswest2);
            System.out.println(instanceInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static BrokerCapacityInfo getBrokerCapacityInfoDefault(AwsConstants.InstanceType instanceType) throws IOException {
        Map<Resource, Double> capacity = new HashMap<>();
        capacity.put(Resource.CPU, 32 * 20 * 100.0);
        Double networkLimitKBps = AwsConstants.getNetworkLimit(instanceType);
        capacity.put(Resource.NW_IN, networkLimitKBps);
        capacity.put(Resource.NW_OUT, networkLimitKBps);

        double ebsSizeMB = lookupEbsSize(Collections.emptyList(), instanceType, AwsConstants.Account.test, AwsConstants.Region.useast1);
        capacity.put(Resource.DISK, ebsSizeMB);
        return new BrokerCapacityInfo(capacity);
    }

    public static BrokerCapacityInfo getBrokerCapacityInfo(String instanceName, String account, String region) throws IOException {
        AwsConstants.Account awsAccount = AwsConstants.Account.valueOf(account);
        AwsConstants.Region awsRegion = AwsConstants.Region.get(region);
        InstanceInfo instanceInfo = EddaClient.getInstanceInfo(instanceName, awsAccount, awsRegion);
        CpuOptions cpu = instanceInfo.getCpuOptions();
        int vCPU = cpu.getCoreCount() * cpu.getThreadsPerCore();
        Map<Resource, Double> capacity = new HashMap<>();
        capacity.put(Resource.CPU, vCPU * 100.0);
        AwsConstants.InstanceType instanceType = AwsConstants.InstanceType.get(instanceInfo.getInstanceType());
        Double networkLimitKBps = AwsConstants.getNetworkLimit(instanceType);
        capacity.put(Resource.NW_IN, networkLimitKBps);
        capacity.put(Resource.NW_OUT, networkLimitKBps);

        double ebsSizeMB = lookupEbsSize(instanceInfo.getBlockDeviceMappings(), instanceType, awsAccount, awsRegion);
        capacity.put(Resource.DISK, ebsSizeMB);
        return new BrokerCapacityInfo(capacity);
    }

    private static double lookupEbsSize(List<BlockDeviceMapping> blockDeviceMappingList,
                                        AwsConstants.InstanceType instanceType,
                                        AwsConstants.Account account,
                                        AwsConstants.Region region) throws IOException {
        Optional<String> volumeId = blockDeviceMappingList
                .stream()
                .filter(m -> m.getDeviceName().equals("sdf")
                        && m.getEbs().getStatus().equals("attached"))
                .map(m -> m.getEbs().getVolumeId())
                .findFirst();
        if (volumeId.isPresent()) {
            int sizeInMB = EddaClient.getVolumeInfo(volumeId.get(), account, region).getSize() * 1000;
            return sizeInMB;
        }

        if (INSTANCE_TYPE_TO_DISK_SIZE_MAP.containsKey(instanceType)) {
            logger.info("using disk size {} MB for instance type {}", INSTANCE_TYPE_TO_DISK_SIZE_MAP.get(instanceType), instanceType);
            return INSTANCE_TYPE_TO_DISK_SIZE_MAP.get(instanceType);
        } else {
            logger.error("failed to lookup EBS size as volumeId not present {}, will default to 1000 GB", blockDeviceMappingList);
            return DEFAULT_DISK_SIZE_MB;
        }
    }

}
