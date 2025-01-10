/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import java.util.Objects;

public class PrincipalName {
    private final String _primary;
    private final String _instance;
    private final String _realm;

    public PrincipalName(String primary, String instance, String realm) {
        _primary = Objects.requireNonNull(primary, "primary must not be null");
        _instance = instance;
        _realm = realm;
    }

    public PrincipalName(String primary) {
        _primary = Objects.requireNonNull(primary, "primary must not be null");
        _instance = null;
        _realm = null;
    }

    public String getPrimary() {
        return _primary;
    }

    public String getInstance() {
        return _instance;
    }

    public String getRealm() {
        return _realm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !Objects.equals(getClass(), o.getClass())) {
            return false;
        }
        PrincipalName principalName = (PrincipalName) o;
        return _primary.equals(principalName._primary) && Objects.equals(_instance, principalName._instance)
                && Objects.equals(_realm, principalName._realm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_primary, _instance, _realm);
    }

    @Override
    public String toString() {
        return "PrincipalName{"
                + "primary='" + _primary + '\''
                + ", instance='" + _instance + '\''
                + ", realm='" + _realm + '\''
                + '}';
    }
}
