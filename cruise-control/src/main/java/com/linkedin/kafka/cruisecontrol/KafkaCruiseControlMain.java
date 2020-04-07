/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The main class to run Kafka Cruise Control.
 */
public class KafkaCruiseControlMain {

  private KafkaCruiseControlMain() { }

  /**
   * The main function to run Cruise Control.
   * @param args Arguments passed while starting Cruise Control.
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException(
              String.format("USAGE: java %s cruisecontrol.properties [port] [ipaddress|hostname]",
                      KafkaCruiseControlMain.class.getSimpleName()));
    }
    KafkaCruiseControlConfig config = readConfig(args[0]);
    Integer port = parsePort(args, config);
    String hostname = parseHostname(args, config);
    KafkaCruiseControlApp app = new KafkaCruiseControlApp(config, port, hostname);
    app.registerShutdownHook();
    app.start();
  }

  private static KafkaCruiseControlConfig readConfig(String propertiesFile) throws IOException {
    Properties props = new Properties();
    try (InputStream propStream = new FileInputStream(propertiesFile)) {
      props.load(propStream);
    }
    return new KafkaCruiseControlConfig(props);
  }

  private static Integer parsePort(String[] args, KafkaCruiseControlConfig config) {
    if (args.length > 1) {
      return Integer.parseInt(args[1]);
    } else {
      return config.getInt(WebServerConfig.WEBSERVER_HTTP_PORT_CONFIG);
    }
  }

  private static String parseHostname(String[] args, KafkaCruiseControlConfig config) {
    if (args.length > 2) {
      return args[2];
    } else {
      return config.getString(WebServerConfig.WEBSERVER_HTTP_ADDRESS_CONFIG);
    }
  }

}
