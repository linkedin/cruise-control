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

public class ZKConfigUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKConfigUtils.class);

    private static final Map<String, String> zkProperties = new HashMap<>();
    static {
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_CNXN_SOCKET_CONFIG, ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_LOCATION_CONFIG, "zookeeper.ssl.keyStore.location");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_PASSWORD_CONFIG, "zookeeper.ssl.keyStore.password");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_TYPE_CONFIG, "zookeeper.ssl.keyStore.type");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_LOCATION_CONFIG, "zookeeper.ssl.trustStore.location");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD_CONFIG, "zookeeper.ssl.trustStore.password");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_TYPE_CONFIG, "zookeeper.ssl.trustStore.type");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_PROTOCOL_CONFIG, "zookeeper.ssl.protocol");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_ENABLED_PROTOCOLS_CONFIG, "zookeeper.ssl.enabledProtocols");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_CIPHER_SUITES_CONFIG, "zookeeper.ssl.ciphersuites");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "zookeeper.ssl.hostnameVerification");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_CRL_ENABLE_CONFIG, "zookeeper.ssl.crl");
        zkProperties.put(ExecutorConfig.ZOOKEEPER_SSL_OCSP_ENABLE_CONFIG, "zookeeper.ssl.ocsp");
    }

    public static ZKClientConfig zkClientConfigFromKafkaConfig(KafkaCruiseControlConfig config) {
        if (!config.getBoolean(ExecutorConfig.ZOOKEEPER_SSL_CLIENT_ENABLE_CONFIG)) {
            return new ZKClientConfig();
        } else {
            ZKClientConfig clientConfig = new ZKClientConfig();
            clientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_CNXN_SOCKET_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_LOCATION_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_PASSWORD_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_KEYSTORE_TYPE_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_LOCATION_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_TRUSTSTORE_TYPE_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_PROTOCOL_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_ENABLED_PROTOCOLS_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_CIPHER_SUITES_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_CRL_ENABLE_CONFIG, config);
            setZooKeeperClientProperty(clientConfig, ExecutorConfig.ZOOKEEPER_SSL_OCSP_ENABLE_CONFIG, config);
            return clientConfig;
        }
    }

    private static void setZooKeeperClientProperty(ZKClientConfig clientConfig, String prop, KafkaCruiseControlConfig config) {
        if (zkProperties.containsKey(prop)) {
            ConfigDef.Type type = config.typeOf(prop);
            switch (type) {
                case STRING:
                    String svalue = config.getString(prop);
                    if (svalue != null)
                        clientConfig.setProperty(zkProperties.get(prop), svalue);
                    break;
                case PASSWORD:
                    Password pvalue = config.getPassword(prop);
                    if (pvalue != null)
                        clientConfig.setProperty(zkProperties.get(prop), pvalue.value());
                    break;
                case LIST:
                    List<String> lvalue = config.getList(prop);
                    if (lvalue != null)
                        clientConfig.setProperty(zkProperties.get(prop), String.join(",", lvalue));
                    break;
                case BOOLEAN:
                    Boolean bvalue = config.getBoolean(prop);
                    if (bvalue != null)
                        clientConfig.setProperty(zkProperties.get(prop), bvalue.toString());
                    break;
                default:
                    LOGGER.error("Unexpected type {} for property {}", type, prop);
            }
        }
    }
}
