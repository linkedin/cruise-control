/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoSecurityProvider;
import org.apache.kerby.kerberos.kerb.KrbException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.AUTH_CREDENTIALS_FILE;

public abstract class SpnegoIntegrationTestHarness extends CruiseControlIntegrationTestHarness {

  public static final String REALM = "LINKEDINTEST.COM";
  public static final String CC_TEST_ADMIN = "ccTestAdmin";
  public static final String CLIENT_PRINCIPAL = CC_TEST_ADMIN + "/localhost";
  public static final String SOME_OTHER_SERVICE_PRINCIPAL = "someotherservice/localhost";
  public static final String SPNEGO_SERVICE_PRINCIPAL = "HTTP/localhost";

  protected final MiniKdc _miniKdc;

  /**
   * Instantiates a new object.
   * @throws KrbException
   */
  public SpnegoIntegrationTestHarness() throws KrbException {
    _miniKdc = new MiniKdc(REALM, principals());
  }

  /**
   * Collects a list of principals that will be initialized by the KDC.
   * @return a list of Strings representing the principals.
   */
  public List<String> principals() {
    List<String> principals = new ArrayList<>();
    principals.add(CLIENT_PRINCIPAL);
    principals.add(SPNEGO_SERVICE_PRINCIPAL);
    principals.add(SOME_OTHER_SERVICE_PRINCIPAL);
    return principals;
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG, true);
    configs.put(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG, SpnegoSecurityProvider.class);
    configs.put(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG,
        Objects.requireNonNull(this.getClass().getClassLoader().getResource(AUTH_CREDENTIALS_FILE)).getPath());
    configs.put(WebServerConfig.SPNEGO_PRINCIPAL_CONFIG, SPNEGO_SERVICE_PRINCIPAL + "@" + REALM);
    configs.put(WebServerConfig.SPNEGO_KEYTAB_FILE_CONFIG, _miniKdc.keytab().getAbsolutePath());

    return configs;
  }

  @Override
  public void start() throws Exception {
    _miniKdc.start();
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    try {
      _miniKdc.stop();
    } catch (KrbException e) {
      throw new RuntimeException();
    }
  }
}
