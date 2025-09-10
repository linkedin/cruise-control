/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlApp;
import com.linkedin.kafka.cruisecontrol.servlet.security.MiniKdc;
import jakarta.servlet.http.HttpServletResponse;
import javax.security.auth.Subject;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.PrivilegedAction;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static org.junit.Assert.assertEquals;

/**
 * A test util class.
 */
public final class SpnegoSecurityProviderTestUtils {

    private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;

    private SpnegoSecurityProviderTestUtils() {

    }

    public static void testSuccessfulAuthentication(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        Subject subject = miniKdc.loginAs(principal);
        Subject.doAs(subject, (PrivilegedAction<Object>) () -> {
            try {
                HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(app.serverUrl())
                        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
                assertEquals(HttpServletResponse.SC_OK, stateEndpointConnection.getResponseCode());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    public static void testNotAdminServiceLogin(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        Subject subject = miniKdc.loginAs(principal);
        Subject.doAs(subject, (PrivilegedAction<Object>) () -> {
            HttpURLConnection stateEndpointConnection;
            try {
                stateEndpointConnection = (HttpURLConnection) new URI(app.serverUrl())
                        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
                assertEquals(HttpServletResponse.SC_UNAUTHORIZED, stateEndpointConnection.getResponseCode());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

}
