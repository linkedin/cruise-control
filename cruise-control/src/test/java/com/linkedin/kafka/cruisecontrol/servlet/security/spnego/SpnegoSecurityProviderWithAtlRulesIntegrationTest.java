/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.SpnegoIntegrationTestHarness;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SpnegoSecurityProviderWithAtlRulesIntegrationTest extends SpnegoIntegrationTestHarness {

    private static final String AUTH_ATL_RULES_CREDENTIALS_FILE = "auth-atlrules.credentials";
    private static final List<String> ATL_RULES = Collections.singletonList("RULE:[2:$1@$0](.*@.*)s/@.*/foo/");

    public SpnegoSecurityProviderWithAtlRulesIntegrationTest() throws KrbException {
    }

    /**
     * Initializes the test environment.
     * @throws Exception
     */
    @Before
    public void setup() throws Exception {
        start();
    }

    /**
     * Stops the test environment.
     */
    @After
    public void teardown() {
        stop();
    }

    @Override
    protected Map<String, Object> withConfigs() {
        Map<String, Object> configs = super.withConfigs();
        configs.put(WebServerConfig.SPNEGO_PRINCIPAL_TO_LOCAL_RULES_CONFIG, ATL_RULES);
        configs.put(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG,
                Objects.requireNonNull(getClass().getClassLoader().getResource(AUTH_ATL_RULES_CREDENTIALS_FILE)).getPath());
        return configs;
    }

    @Test
    public void testSuccessfulAuthentication() throws Exception {
        SpnegoSecurityProviderTestUtils.testSuccessfulAuthentication(_miniKdc, _app, CLIENT_PRINCIPAL + '@' + REALM);
    }

    @Test
    public void testNotAdminServiceLogin() throws Exception {
        SpnegoSecurityProviderTestUtils.testNotAdminServiceLogin(_miniKdc, _app, SOME_OTHER_SERVICE_PRINCIPAL + '@' + REALM);
    }

}
