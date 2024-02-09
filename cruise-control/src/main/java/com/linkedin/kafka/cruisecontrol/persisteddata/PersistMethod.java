/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Defines the persist method types of persistent data.
 */
public enum PersistMethod {
    MEMORY,
    KAFKA;

    /**
     * Parse the string version of a persist method into the enum.
     *
     * @param value The value to parse.
     * @return The correctly parsed enum.
     */
    public static PersistMethod fromString(String value) {
        return valueOf(value.toUpperCase());
    }

    /**
     * Get the lower case string values of all persist methods.
     *
     * @return The list of all the persist methods as lower case strings.
     */
    public static List<String> stringValues() {
        return Arrays.stream(PersistMethod.values())
                .map(persistMethod -> persistMethod.toString().toLowerCase())
                .sorted()
                .collect(Collectors.toList());
    }
}
