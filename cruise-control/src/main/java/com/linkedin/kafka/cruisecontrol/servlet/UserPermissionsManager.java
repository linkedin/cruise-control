/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.Collections;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.eclipse.jetty.security.RolePrincipal;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.PropertyUserStore;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The util class for User Permissions
 */
public class UserPermissionsManager {
    private static final Logger LOG = LoggerFactory.getLogger(UserPermissionsManager.class);

    private final KafkaCruiseControlConfig _config;
    private final Map<String, Set<String>> _rolesPerUsers;

    public UserPermissionsManager(KafkaCruiseControlConfig config) {
        _config = config;
        _rolesPerUsers = createRolesPerUsersMap();
    }

    /**
     * Builds a map between existing users and their roles in Cruise Control
     *
     * @return a map of usernames -> their assigned roles
     */
    private Map<String, Set<String>> createRolesPerUsersMap() {
        Map<String, Set<String>> rolesPerUsers = new HashMap<>();
        boolean securityEnabled = _config.getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG);
        if (securityEnabled) {
            String privilegesFilePath = _config.getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG);
            Resource resource = ResourceFactory.of(new ResourceHandler()).newResource(privilegesFilePath);
            UserStore userStore = createUserStoreFromResource(resource);
            startUserStore(userStore);

            Set<String> userNames = parseUsernames(resource);

            for (String user : userNames) {
                Set<RolePrincipal> roles = new HashSet<>(userStore.getRolePrincipals(user));

                Set<String> roleNames = roles.stream()
                        .map(RolePrincipal::getName)
                        .map(String::toUpperCase)
                        .collect(Collectors.toSet());
                rolesPerUsers.put(user, roleNames);
            }
            stopUserStore(userStore);
        }
        return rolesPerUsers;
    }

    private void startUserStore(UserStore userStore) {
        try {
            userStore.start();
        } catch (Exception e) {
            LOG.info("UserPermissionsManager user store cannot be started, all permissions queries will contain disallowed roles.");
            LOG.warn("UserPermissionsManager is unable to read the file defined by the following configuration: {}",
                    WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG, e);
        }
    }

    private void stopUserStore(UserStore userStore) {
        try {
            userStore.stop();
        } catch (Exception e) {
            LOG.info("UserPermissionsManager user store cannot be stopped. Exception:\n", e);
        }
    }

    /** Returns the set of roles for a given username
     *
     * @param userName the username
     * @return either Set with roles or an empty set
     */
    public Set<String> getRolesBy(String userName) {
        return _rolesPerUsers.getOrDefault(userName, Collections.emptySet());
    }

    /** Creates UserStore from an external file
     *
     * @param privilegedResource a filepath containing user privileges information
     * @return a UserStore object
     */
    private UserStore createUserStoreFromResource(Resource privilegedResource) {
        PropertyUserStore userStore = new PropertyUserStore();
        userStore.setConfig(privilegedResource);
        return userStore;
    }

    /** Creates a set of usernames from a Resource
     *
     * @param resource a Resource containing user privileges information
     * @return a Set of usernames parsed from the Resource
     */
    private static Set<String> parseUsernames(Resource resource) {
        if (!resource.exists() || !resource.isReadable()) {
            return Set.of();
        }
        Set<String> usernames = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.newInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                int colonIndex = line.indexOf(':');
                if (colonIndex != -1) {
                    String username = line.substring(0, colonIndex).trim();
                    if (!username.isEmpty()) {
                        usernames.add(username);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read usernames from " + resource, e);
        }
        return usernames;
    }
}
