/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PersistedMapTest {

    private static final String KEY_1 = "key1";
    private static final String VALUE_1 = "value1";
    private static final String KEY_2 = "key2";
    private static final String VALUE_2 = "value2";

    private Map<String, String> _backingMap;
    private PersistedMap _persistedMap;

    /**
     * Setup for the tests.
     */
    @Before
    public void setUp() {
        _backingMap = new HashMap<>();
        _persistedMap = new PersistedMap(this._backingMap);
    }

    /**
     * The mapping is removed by putting a null value for the key.
     */
    @Test
    public void removeMeansTheValueIsNullForTheKey() {
        _backingMap.put(KEY_1, VALUE_1);
        _persistedMap.remove(KEY_1);
        assertThat(_backingMap.get(KEY_1), is(nullValue()));
    }

    /**
     * All the keys and values are produced in order.
     */
    @Test
    public void putAllPutsEachKeyAndValue() {
        final Map<String, String> data = Map.of(
                KEY_1, VALUE_1,
                KEY_2, VALUE_2);
        _persistedMap.putAll(data);
        assertThat(_backingMap, is(data));
    }

    /**
     * Clear should remove all the values.
     */
    @Test
    public void clearRemovesAllTheValues() {
        final Map<String, String> data = Map.of(
                KEY_1, VALUE_1,
                KEY_2, VALUE_2);
        _backingMap.putAll(data);
        _persistedMap.clear();
        assertThat(_backingMap.get(KEY_1), is(nullValue()));
        assertThat(_backingMap.get(KEY_2), is(nullValue()));
    }
}
