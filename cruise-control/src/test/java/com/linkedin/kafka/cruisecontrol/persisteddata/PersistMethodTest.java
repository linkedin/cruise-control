/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.List;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PersistMethodTest {

    /**
     * Ensure fromString accepts valid values ignoring case.
     */
    @Test
    public void fromStringAcceptsValidValuesIgnoringCase() {
        assertThat(PersistMethod.fromString("kafka"), is(PersistMethod.KAFKA));
        assertThat(PersistMethod.fromString("Kafka"), is(PersistMethod.KAFKA));
    }

    /**
     * Ensure fromString rejects invalid values.
     */
    @Test(expected = IllegalArgumentException.class)
    public void fromStringRejectsInvalidValues() {
        PersistMethod.fromString("invalid");
    }

    /**
     * Ensure stringValues gets all possible backing method options.
     */
    @Test
    public void stringValuesGetsAllEnumsLowercaseAndSorted() {
        List<String> expected = List.of("kafka", "memory");
        List<String> actual = PersistMethod.stringValues();
        assertThat(actual, is(expected));
    }
}
