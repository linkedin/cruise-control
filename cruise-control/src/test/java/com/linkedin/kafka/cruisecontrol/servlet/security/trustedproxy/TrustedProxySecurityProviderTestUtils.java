/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlApp;
import com.linkedin.kafka.cruisecontrol.servlet.security.MiniKdc;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletResponse;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.PrivilegedAction;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * A test util class.
 */
public final class TrustedProxySecurityProviderTestUtils {

    private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;

    private TrustedProxySecurityProviderTestUtils() {

    }

    public static void testSuccessfulAuthentication(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal, String doAs) throws Exception {
        Subject subject = miniKdc.loginAs(principal);
        Subject.doAs(subject, (PrivilegedAction<Object>) () -> {
            try {
                String endpoint = CRUISE_CONTROL_STATE_ENDPOINT + '?' + DO_AS + '=' + doAs;
                HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(app.serverUrl())
                        .resolve(endpoint).toURL().openConnection();
                assertEquals(HttpServletResponse.SC_OK, stateEndpointConnection.getResponseCode());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    public static void testNoDoAsParameter(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        Subject subject = miniKdc.loginAs(principal);
        Subject.doAs(subject, (PrivilegedAction<Object>) () -> {

            HttpURLConnection stateEndpointConnection;
            try {
                stateEndpointConnection = (HttpURLConnection) new URI(app.serverUrl())
                        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // There is a bug in the Jetty implementation, and it doesn't seem to handle the connection
            // properly in case of an error, so it somehow doesn't send a response code. To work this around
            // I catch the RuntimeException that it throws.
            assertThrows(RuntimeException.class, stateEndpointConnection::getResponseCode);
            return null;
        });
    }

    public static void testNotAdminServiceLogin(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal, String doAs) throws Exception {
        Subject subject = miniKdc.loginAs(principal);
        Subject.doAs(subject, (PrivilegedAction<Object>) () -> {

            String endpoint = CRUISE_CONTROL_STATE_ENDPOINT + '?' + DO_AS + '=' + doAs;
            HttpURLConnection stateEndpointConnection;
            try {
                stateEndpointConnection = (HttpURLConnection) new URI(app.serverUrl())
                        .resolve(endpoint).toURL().openConnection();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // There is a bug in the Jetty implementation, and it doesn't seem to handle the connection
            // properly in case of an error, so it somehow doesn't send a response code. To work this around
            // I catch the RuntimeException that it throws.
            assertThrows(RuntimeException.class, stateEndpointConnection::getResponseCode);
            return null;
        });
    }
    
    public static void testSuccessfulFallbackAuthentication(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        Subject subject = miniKdc.loginAs(principal);
        Subject.doAs(subject, (PrivilegedAction<Object>) () -> {

            HttpURLConnection stateEndpointConnection;
            try {
                stateEndpointConnection = (HttpURLConnection) new URI(app.serverUrl())
                        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
                assertEquals(HttpServletResponse.SC_OK, stateEndpointConnection.getResponseCode());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    public static void testUnsuccessfulFallbackAuthentication(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        Subject subject = miniKdc.loginAs(principal);
        Subject.doAs(subject, (PrivilegedAction<Object>) () -> {

            HttpURLConnection stateEndpointConnection;
            try {
                stateEndpointConnection = (HttpURLConnection) new URI(app.serverUrl())
                        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // There is a bug in the Jetty implementation, and it doesn't seem to handle the connection
            // properly in case of an error, so it somehow doesn't send a response code. To work this around
            // I catch the RuntimeException that it throws.
            assertThrows(RuntimeException.class, stateEndpointConnection::getResponseCode);
            return null;
        });
    }

}
