/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.common.config.types;

/**
 * A wrapper class for passwords to hide them while logging a config
 */
public class Password {

    public static final String HIDDEN = "[hidden]";

    private final String _value;

    /**
     * Construct a new Password object
     * @param value The value of a password
     */
    public Password(String value) {
        _value = value;
    }

    @Override
    public int hashCode() {
        return _value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Password)) {
            return false;
        }
        Password other = (Password) obj;
        return _value.equals(other._value);
    }

    /**
     * Returns hidden password string
     *
     * @return hidden password string
     */
    @Override
    public String toString() {
        return HIDDEN;
    }

    /**
     * Returns real password string
     *
     * @return Real password string
     */
    public String value() {
        return _value;
    }
}
