/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Defines the backing method types of persistent data.
 */
public enum BackingMethod {
    MEMORY,
    KAFKA;

    /**
     * Parse the string version of a backing method into the enum.
     *
     * @param value The value to parse.
     * @return The correctly parsed enum.
     */
    public static BackingMethod fromString(String value) {
        return valueOf(value.toUpperCase());
    }

    /**
     * Get the lower case string values of all backing methods.
     *
     * @return The list of all the backing methods as lower case strings.
     */
    public static List<String> stringValues() {
        return Arrays.stream(BackingMethod.values())
                .map(backingMethod -> backingMethod.toString().toLowerCase())
                .sorted()
                .collect(Collectors.toList());
    }
}
