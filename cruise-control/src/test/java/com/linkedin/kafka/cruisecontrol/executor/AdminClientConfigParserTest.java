/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.Assert;
import org.junit.Test;


public class AdminClientConfigParserTest {

  @Test
  public void testParseAdminClientRequestTimeoutMs() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    Integer expectedAdminClientTimeoutMs = (int) TimeUnit.SECONDS.toMillis(101);
    properties.put(ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(expectedAdminClientTimeoutMs));
    KafkaCruiseControlConfig cruiseControlConfig = new KafkaCruiseControlConfig(properties);
    Map<String, Object> parsedAdminClientConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(cruiseControlConfig);
    Assert.assertEquals(expectedAdminClientTimeoutMs, parsedAdminClientConfigs.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
  }

  @Test
  public void testParseDefaultAdminClientRequestTimeoutMs() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig cruiseControlConfig = new KafkaCruiseControlConfig(properties);
    Map<String, Object> parsedAdminClientConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(cruiseControlConfig);
    Assert.assertEquals(ExecutorConfig.DEFAULT_ADMIN_CLIENT_REQUEST_TIMEOUT_MS,
                        parsedAdminClientConfigs.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
  }
}
