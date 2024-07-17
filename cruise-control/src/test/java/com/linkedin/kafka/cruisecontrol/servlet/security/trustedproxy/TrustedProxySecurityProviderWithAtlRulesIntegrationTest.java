/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.SpnegoIntegrationTestHarness;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TrustedProxySecurityProviderWithAtlRulesIntegrationTest extends SpnegoIntegrationTestHarness {

    private static final String AUTH_ATL_RULES_CREDENTIALS_FILE = "auth-atlrules.credentials";
    private static final List<String> ATL_RULES = Collections.singletonList("RULE:[2:$1@$0](.*@.*)s/@.*/foo/");
    private static final String AUTH_SERVICE_NAME = "testauthservice";
    private static final String AUTH_SERVICE_PRINCIPAL = AUTH_SERVICE_NAME + "/localhost";

    public TrustedProxySecurityProviderWithAtlRulesIntegrationTest() throws KrbException {
    }

    @Override
    public List<String> principals() {
        List<String> principals = super.principals();
        principals.add(AUTH_SERVICE_PRINCIPAL);
        return principals;
    }

    @Override
    protected Map<String, Object> withConfigs() {
        Map<String, Object> configs = super.withConfigs();
        configs.put(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG, TrustedProxySecurityProvider.class);
        configs.put(WebServerConfig.TRUSTED_PROXY_SERVICES_CONFIG, AUTH_SERVICE_NAME + "foo");
        configs.put(WebServerConfig.SPNEGO_PRINCIPAL_TO_LOCAL_RULES_CONFIG, ATL_RULES);

        return configs;
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

    @Test
    public void testSuccessfulAuthentication() throws Exception {
        TrustedProxySecurityProviderTestUtils.testSuccessfulAuthentication(_miniKdc, _app, AUTH_SERVICE_PRINCIPAL + '@' + REALM, CC_TEST_ADMIN);
    }

    @Test
    public void testNoDoAsParameter() throws Exception {
        TrustedProxySecurityProviderTestUtils.testNoDoAsParameter(_miniKdc, _app, AUTH_SERVICE_PRINCIPAL + '@' + REALM);
    }

    @Test
    public void testNotAdminServiceLogin() throws Exception {
        TrustedProxySecurityProviderTestUtils.testNotAdminServiceLogin(_miniKdc, _app, SOME_OTHER_SERVICE_PRINCIPAL + '@' + REALM, CC_TEST_ADMIN);
    }
}
