/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.util.security.Credential;

public final class SecurityUtils {
    public static final Credential NO_CREDENTIAL = new Credential() {
        @Override
        public boolean check(Object credentials) {
            return false;
        }
    };

    private SecurityUtils() {
    }
}
