/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import static org.junit.Assert.assertThrows;

import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.easymock.EasyMock.reset;


public class WebserverUrlPrefixConfigTest extends AbstractConfigTest {

  @Test
  public void testWebserverUrlPrefixConfigsCorrectSetup() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(GET_STRING_METHOD_NAME)
        .createNiceMock();

    expect(config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG)).andReturn("a/b/c/d/*");
    expect(config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG)).andReturn("a/b/c/*");
    replay(config);
    config.sanityCheckWebServerUrlPrefix();
    verify(config);
  }

  @Test
  public void testWebserverUrlPrefixConfigCorrectSetupOnDefault() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(GET_STRING_METHOD_NAME)
        .createNiceMock();

    expect(config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG)).andReturn(WebServerConfig.DEFAULT_WEBSERVER_API_URLPREFIX);
    expect(config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG)).andReturn(WebServerConfig.DEFAULT_WEBSERVER_UI_URLPREFIX);
    replay(config);
    config.sanityCheckWebServerUrlPrefix();
    verify(config);
  }

  @Test
  public void testWebserverUrlPrefixConfigThrowsExceptionOnInvalidValue() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(GET_STRING_METHOD_NAME)
        .createNiceMock();

    expect(config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG)).andReturn("a/b/c/d");
    replay(config);
    assertThrows(ConfigException.class, config::sanityCheckWebServerUrlPrefix);
    verify(config);
    reset(config);

    expect(config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG)).andReturn("a/b/c/d/*"); // Valid value
    expect(config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG)).andReturn("a/b/c/");     // Invalid value
    replay(config);
    assertThrows(ConfigException.class, config::sanityCheckWebServerUrlPrefix);
    verify(config);
  }
}
