/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;


/**
 * Util class for convenience.
 */
public class KafkaCruiseControlUtils {

  private KafkaCruiseControlUtils() {

  }

  /**
   * Format the timestamp from long to a human readable string.
   */
  public static String toDateString(long time) {
    DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    return format.format(new Date(time));
  }

  /**
   * Get a configuration and throw exception if the configuration was not provided.
   * @param configs the config map.
   * @param configName the config to get.
   * @return the configuration string.
   */
  public static String getRequiredConfig(Map<String, ?> configs, String configName) {
    String value = (String) configs.get(configName);
    if (value == null) {
      throw new ConfigException(String.format("Configuration %s must be provided.", configName));
    }
    return value;
  }

  /**
   * Set SSL configs to a property.
   */
  public static void setSslConfigs(Properties props, Map<String, ?> config) {
    putIfAbsent(props, config, SslConfigs.SSL_CIPHER_SUITES_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_KEY_PASSWORD_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_PROTOCOL_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_PROVIDER_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
    putIfAbsent(props, config, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
  }

  public static void putIfAbsent(Properties props, Map<String, ?> config, String name) {
    if (config.containsKey(name)) {
      props.setProperty(name, (String) config.get(name));
    }
  }

  public static void closeZkUtilsWithTimeout(ZkUtils zkUtils, long timeoutMs) {
    Thread t = new Thread() {
      @Override
      public void run() {
        zkUtils.close();
      }
    };
    t.setDaemon(true);
    t.start();
    try {
      t.join(timeoutMs);
    } catch (InterruptedException e) {
      // let it go
    }
    if (t.isAlive()) {
      t.interrupt();
    }
  }
}
