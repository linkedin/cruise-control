/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import javax.validation.constraints.NotNull;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UserPermissionsManagerTest {
    private static final String AUTH_CREDENTIALS_FILE = "auth-permissions.credentials";

    @Test
    public void testGetRolesPerUsers() {
        KafkaCruiseControlConfig cruiseControlConfig = setupConfigurations(true);

        UserPermissionsManager userPermissionsManager = new UserPermissionsManager(cruiseControlConfig);

        assertEquals(new HashSet<>(Arrays.asList("ADMIN", "USER")), userPermissionsManager.getRolesBy("ccTestAdmin"));
        assertEquals(new HashSet<>(Arrays.asList("USER", "VIEWER")), userPermissionsManager.getRolesBy("ccTestUser"));
        assertEquals(Collections.singleton("VIEWER"), userPermissionsManager.getRolesBy("ccTestUser2"));
        assertEquals(Collections.singleton("ADMIN"), userPermissionsManager.getRolesBy("kafka"));
    }

    @Test
    public void testGetRolesWhenUserUnknown() {
        KafkaCruiseControlConfig cruiseControlConfig = setupConfigurations(false);

        UserPermissionsManager userPermissionsManager = new UserPermissionsManager(cruiseControlConfig);

        assertTrue(userPermissionsManager.getRolesBy("UNKNOWN").isEmpty());
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
}
