/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.BasicSecurityProvider;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class SecurityAndSslConfigTest {

  @Rule
  public ExpectedException _expectedException = ExpectedException.none();

  @Test
  public void testSslConfigsSetUpThrowsExceptionOnNullKeystoreLocation() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(ConfigTestUtils.GET_BOOLEAN_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.GET_STRING_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.HASH_CODE_METHOD_NAME)
        .createNiceMock();
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG)).andReturn(true);
    replay(config);
    _expectedException.expect(ConfigException.class);
    config.sanityCheckSecurity();
    verify(config);
  }

  @Test
  public void testSslConfigsSetUpThrowsExceptionOnNullSslProtocol() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(ConfigTestUtils.GET_BOOLEAN_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.GET_STRING_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.HASH_CODE_METHOD_NAME)
        .createNiceMock();
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG)).andReturn(true);
    expect(config.getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG))
        .andReturn(getClass().getClassLoader().getResource("ssl_integration_test.keystore").toString());
    expect(config.getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_PASSWORD_CONFIG))
        .andReturn("jetty");
    replay(config);
    _expectedException.expect(ConfigException.class);
    config.sanityCheckSecurity();
    verify(config);
  }

  @Test
  public void testSslConfigsCorrectSetup() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(ConfigTestUtils.GET_BOOLEAN_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.GET_STRING_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.HASH_CODE_METHOD_NAME)
        .createNiceMock();
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG)).andReturn(true);
    expect(config.getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG))
        .andReturn(getClass().getClassLoader().getResource("ssl_integration_test.keystore").toString());
    expect(config.getString(WebServerConfig.WEBSERVER_SSL_PROTOCOL_CONFIG))
        .andReturn("TLS");
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG)).andReturn(false);

    replay(config);
    config.sanityCheckSecurity();
    verify(config);
  }

  @Test
  public void testAuthConfigThrowsExceptionOnNullSecurityProvider() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(ConfigTestUtils.GET_BOOLEAN_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.GET_CLASS_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.HASH_CODE_METHOD_NAME)
        .createNiceMock();
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG)).andReturn(false);
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG)).andReturn(true);

    replay(config);
    _expectedException.expect(ConfigException.class);
    config.sanityCheckSecurity();
    verify(config);
  }

  @Test
  public void testAuthConfigThrowsExceptionOnNullCredentialsFile() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(ConfigTestUtils.GET_BOOLEAN_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.GET_STRING_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.GET_CLASS_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.HASH_CODE_METHOD_NAME)
        .createNiceMock();
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG)).andReturn(false);
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG)).andReturn(true);
    expect(config.getClass(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG)).andReturn((Class) BasicSecurityProvider.class);

    replay(config);
    _expectedException.expect(ConfigException.class);
    config.sanityCheckSecurity();
    verify(config);
  }

  @Test
  public void testAuthConfigCorrectSetup() {
    KafkaCruiseControlConfig config = partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(ConfigTestUtils.GET_BOOLEAN_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.GET_STRING_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.GET_CLASS_METHOD_NAME)
        .addMockedMethod(ConfigTestUtils.HASH_CODE_METHOD_NAME)
        .createNiceMock();
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG)).andReturn(false);
    expect(config.getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG)).andReturn(true);
    expect(config.getClass(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG)).andReturn((Class) BasicSecurityProvider.class);
    expect(config.getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG))
        .andReturn(getClass().getClassLoader().getResource("basic-auth.credentials").getPath());

    replay(config);
    config.sanityCheckSecurity();
    verify(config);
  }
}
