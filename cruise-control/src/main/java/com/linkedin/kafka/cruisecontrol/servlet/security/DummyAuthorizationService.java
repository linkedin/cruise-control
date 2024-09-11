/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.security.Credential;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import java.security.Principal;

public class DummyAuthorizationService implements AuthorizationService {

    private static final Credential NO_CREDENTIAL = new Credential() {
        @Override
        public boolean check(Object credentials) {
            return false;
        }
    };

    @Override
    public UserIdentity getUserIdentity(HttpServletRequest request, String name) {
        return createUserIdentity(name);
    }

    private UserIdentity createUserIdentity(String username) {
        Principal userPrincipal = new SpnegoUserPrincipal(username, "");
        Subject subject = new Subject();
        subject.getPrincipals().add(userPrincipal);
        subject.getPrivateCredentials().add(NO_CREDENTIAL);
        subject.setReadOnly();

        return new SpnegoUserIdentity(subject, userPrincipal, null);
    }

}
