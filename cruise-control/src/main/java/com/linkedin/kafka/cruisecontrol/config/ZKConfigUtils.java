/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ZKConfigUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConfigUtils.class);

    private static final Map<String, String> ZK_PROPERTIES = new HashMap<>();
    static {
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_CNXN_SOCKET_CONFIG, ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_LOCATION_CONFIG, "zookeeper.ssl.keyStore.location");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_PASSWORD_CONFIG, "zookeeper.ssl.keyStore.password");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_TYPE_CONFIG, "zookeeper.ssl.keyStore.type");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_LOCATION_CONFIG, "zookeeper.ssl.trustStore.location");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD_CONFIG, "zookeeper.ssl.trustStore.password");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_TYPE_CONFIG, "zookeeper.ssl.trustStore.type");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_PROTOCOL_CONFIG, "zookeeper.ssl.protocol");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_ENABLED_PROTOCOLS_CONFIG, "zookeeper.ssl.enabledProtocols");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_CIPHER_SUITES_CONFIG, "zookeeper.ssl.ciphersuites");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "zookeeper.ssl.hostnameVerification");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_CRL_ENABLE_CONFIG, "zookeeper.ssl.crl");
        ZK_PROPERTIES.put(ExecutorConfig.ZOOKEEPER_SSL_OCSP_ENABLE_CONFIG, "zookeeper.ssl.ocsp");
    }

    private ZKConfigUtils() {

    }

    /**
     * Builds the ZooKeeper client configuration
     * @param config The Cruise Control configuration
     * @return ZKClientConfig for the ZooKeeper client
     */
    public static ZKClientConfig zkClientConfigFromKafkaConfig(KafkaCruiseControlConfig config) {
        if (!config.getBoolean(ExecutorConfig.ZOOKEEPER_SSL_CLIENT_ENABLE_CONFIG)) {
            return new ZKClientConfig();
        } else {
            ZKClientConfig clientConfig = new ZKClientConfig();
            clientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
            for (String key : ZK_PROPERTIES.keySet()) {
                setZooKeeperClientProperty(clientConfig, key, config);
            }
            return clientConfig;
        }
    }

    private static void setZooKeeperClientProperty(ZKClientConfig clientConfig, String prop, KafkaCruiseControlConfig config) {
        if (ZK_PROPERTIES.containsKey(prop)) {
            ConfigDef.Type type = config.typeOf(prop);
            switch (type) {
                case STRING:
                    String svalue = config.getString(prop);
                    if (svalue != null) {
                        clientConfig.setProperty(ZK_PROPERTIES.get(prop), svalue);
                    }
                    break;
                case PASSWORD:
                    Password pvalue = config.getPassword(prop);
                    if (pvalue != null) {
                        clientConfig.setProperty(ZK_PROPERTIES.get(prop), pvalue.value());
                    }
                    break;
                case LIST:
                    List<String> lvalue = config.getList(prop);
                    if (lvalue != null) {
                        clientConfig.setProperty(ZK_PROPERTIES.get(prop), String.join(",", lvalue));
                    }
                    break;
                case BOOLEAN:
                    Boolean bvalue = config.getBoolean(prop);
                    if (bvalue != null) {
                        clientConfig.setProperty(ZK_PROPERTIES.get(prop), bvalue.toString());
                    }
                    break;
                default:
                    LOGGER.error("Unexpected type {} for property {}", type, prop);
                    break;
            }
        }
    }
}
