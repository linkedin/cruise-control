/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.server.Request;

/**
 * An interface to get roles for a given user.
 */
public interface RoleProvider {
    /**
     * Get the roles for a given user.
     *
     * @param request  the request
     * @param username the username
     * @return the roles for the user or null if no roles found
     */
    String[] rolesFor(Request request, String username);
}
