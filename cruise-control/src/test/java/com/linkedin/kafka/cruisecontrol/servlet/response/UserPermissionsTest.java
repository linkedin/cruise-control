/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserPermissionsManager;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import javax.validation.constraints.NotNull;
import org.junit.Test;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Test to check UserPermissions class works correctly.
 */
public class UserPermissionsTest {
    private static final String AUTH_CREDENTIALS_FILE = "auth-permissions.credentials";
    private UserPermissions _userPermissions;

    private void setupUserPermissions(boolean isSecurityEnabled) {
        KafkaCruiseControlConfig config = setupConfigurations(isSecurityEnabled);
        UserPermissionsManager userPermissionsManager = new UserPermissionsManager(config);
        _userPermissions = new UserPermissions(config, userPermissionsManager);
    }

    @Test
    public void testGetUerPermissionsWhenNoSecurity() {
        setupUserPermissions(false);

        assertThrows("Unable to retrieve privilege information for an unsecure connection",
                UserRequestException.class,
                () -> _userPermissions.getJsonStructure("ANONYMOUS"));
    }

    @Test
    public void testGetUerPermissionsWhenUserIsAdmin() {
        setupUserPermissions(true);

        Set<String> result = _userPermissions.getJsonStructure("ccTestAdmin");

        assertEquals(createExpected(false, true, true), result);
    }

    @Test
    public void testGetUerPermissionsWhenUserIsUser() {
        setupUserPermissions(true);

        Set<String> result = _userPermissions.getJsonStructure("ccTestUser");

        assertEquals(createExpected(true, true, false), result);
    }

    @Test
    public void testGetUerPermissionsWhenUserIsViewer() {
        setupUserPermissions(true);

        Set<String> result = _userPermissions.getJsonStructure("ccTestUser2");

        assertEquals(createExpected(true, false, false), result);
    }

    @NotNull
    private KafkaCruiseControlConfig setupConfigurations(boolean isSecurityEnabled) {
        Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();

        Map<String, Object> configs = new HashMap<>();
        configs.put(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG, isSecurityEnabled);
        configs.put(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG,
                Objects.requireNonNull(this.getClass().getClassLoader().getResource(AUTH_CREDENTIALS_FILE)).getPath());

        properties.putAll(configs);
        return new KafkaCruiseControlConfig(properties);
    }

    @NotNull
    private Set<String> createExpected(boolean isViewer, boolean isUser, boolean isAdmin) {
        Set<String> roles = new HashSet<>();
        if (isViewer) {
            roles.add("VIEWER");
        }
        if (isUser) {
            roles.add("USER");
        }
        if (isAdmin) {
            roles.add("ADMIN");
        }
        return roles;
    }
}
