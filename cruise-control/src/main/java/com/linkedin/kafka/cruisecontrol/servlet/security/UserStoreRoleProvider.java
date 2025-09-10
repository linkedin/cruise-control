/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.RolePrincipal;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.resource.PathResourceFactory;
import org.eclipse.jetty.util.resource.Resource;
import java.nio.file.Path;
import java.util.List;

public class UserStoreRoleProvider extends AbstractLifeCycle implements RoleProvider {

    private final UserStore _userStore;

    public UserStoreRoleProvider(UserStore userStore) {
        _userStore = userStore;
    }

    public UserStoreRoleProvider(String privilegesFilePath) {
        PropertyUserStore store = new PropertyUserStore();
        Resource res = new PathResourceFactory().newResource(Path.of(privilegesFilePath).toUri());
        store.setConfig(res);
        _userStore = store;
    }

    /**
     * Get the roles for a given user.
     *
     * @param request  the request
     * @param username the username
     * @return the roles for the user or null if no roles found
     */
    public String[] rolesFor(Request request, String username) {
        List<RolePrincipal> rolePrincipals = _userStore.getRolePrincipals(username);
        if (rolePrincipals == null || rolePrincipals.isEmpty()) {
            return null;
        }
        return rolePrincipals.stream().map(RolePrincipal::getName).toArray(String[]::new);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        _userStore.start();
    }

    @Override
    protected void doStop() throws Exception {
        try {
            _userStore.stop();
        } finally {
            super.doStop();
        }
    }
}
