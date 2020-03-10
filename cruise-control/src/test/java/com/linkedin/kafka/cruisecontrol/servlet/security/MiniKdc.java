/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

/**
 * A very simple KDC that can be used for testing.
 */
public class MiniKdc {

  private static final String TEMP_DIR_PROPERTY_KEY = "java.io.tmpdir";

  private final SimpleKdcServer _kerbyServer;
  private final File _keytab;
  private final String _realm;
  private List<String> _principals;

  public MiniKdc(String realm, List<String> principals) throws KrbException {
    _kerbyServer = new SimpleKdcServer();
    _realm = realm;
    _principals = principals;
    _keytab = Paths.get(System.getProperty(TEMP_DIR_PROPERTY_KEY), UUID.randomUUID().toString() + ".keytab").toFile();
  }

  public File keytab() {
    return _keytab;
  }

  /**
   * Initializes ans tarts the KDC.
   * @throws KrbException
   * @throws IOException
   */
  public void start() throws KrbException, IOException {
    _kerbyServer.setWorkDir(Files.createTempDirectory("kerby-server-test-harness-").toFile());
    _kerbyServer.setKdcRealm(_realm);
    _kerbyServer.setAllowUdp(false);
    _kerbyServer.init();
    _kerbyServer.start();

    _kerbyServer.createAndExportPrincipals(_keytab, _principals.toArray(new String[]{}));
  }

  /**
   * Stops the KDC.
   * @throws KrbException
   */
  public void stop() throws KrbException {
    _kerbyServer.stop();
  }

  public Subject loginAs(String principal) throws LoginException {
    return JaasKrbUtil.loginUsingKeytab(principal, _keytab);
  }
}
