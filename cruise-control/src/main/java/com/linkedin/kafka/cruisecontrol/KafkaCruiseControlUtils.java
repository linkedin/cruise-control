/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.config.ConfigException;


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
