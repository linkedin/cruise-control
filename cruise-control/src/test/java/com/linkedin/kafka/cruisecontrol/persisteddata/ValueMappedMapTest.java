/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ValueMappedMapTest {

    private static final Integer KEY1 = 1;
    private static final Integer KEY2 = 2;
    private static final Integer EXTERNAL_VALUE1 = 3;
    private static final Integer EXTERNAL_VALUE2 = 6;
    private static final Map<Integer, Integer> EXTERNAL_VALUES_MAP =
            Map.of(KEY1, EXTERNAL_VALUE1, KEY2, EXTERNAL_VALUE2);
    private static final Integer INTERNAL_VALUE1 = 4;
    private static final Integer INTERNAL_VALUE2 = 7;
    private static final Map<Integer, Integer> INTERNAL_VALUES_MAP =
            Map.of(KEY1, INTERNAL_VALUE1, KEY2, INTERNAL_VALUE2);

    private Map<Integer, Integer> _backingMap;
    private Map<Integer, Integer> _mappedValues;

    /**
     * Set a common starting point.
     */
    @Before
    public void beforeTest() {
        this._backingMap = new HashMap<>();
        this._mappedValues = new ValueMappedMap<>(this._backingMap,
                ValueMappedMapTest::incrementInteger, ValueMappedMapTest::decrementInteger);
    }

    // The value mapper function.
    private static Integer incrementInteger(Object o) {
        if (!(o instanceof Integer)) {
            throw new IllegalArgumentException(
                    String.format("Value must be an Integer but was: %s",
                            o.getClass().getSimpleName()));
        }
        return (Integer) o + 1;
    }

    // The value back mapper function.
    private static Integer decrementInteger(Integer i) {
        return i - 1;
    }

    /**
     * Ensure get() maps external values to internal ones and passes calls onto the child map.
     */
    @Test
    public void getAppliesBackMapperToValue() {
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        assertThat(this._mappedValues.get(KEY1), is(EXTERNAL_VALUE1));
    }

    /**
     * Ensure get() doesn't choke on a null value.
     */
    @Test
    public void getHandlesNullValue() {
        assertThat(this._mappedValues.get(KEY1), is(nullValue()));
    }

    /**
     * Ensure put() maps external values to internal ones and passes calls onto the child map.
     */
    @Test
    public void putStoresInternalValue() {
        assertThat(this._mappedValues.put(KEY1, EXTERNAL_VALUE1), is(nullValue()));
        assertThat(this._backingMap.get(KEY1), is(INTERNAL_VALUE1));
    }

    /**
     * Ensure put() maps external values to internal ones and passes calls onto the child map.
     */
    @Test
    public void putBackMapsPreviousValue() {
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        assertThat(this._mappedValues.put(KEY1, EXTERNAL_VALUE1), is(EXTERNAL_VALUE1));
    }

    /**
     * Ensure containsKey() passes calls onto the child map.
     */
    @Test
    public void containsKeyPassesCallToChildMap() {
        assertThat(this._mappedValues.containsKey(KEY1), is(false));
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        assertThat(this._mappedValues.containsKey(KEY1), is(true));
    }

    /**
     * Ensure remove() maps external values to internal ones and passes calls onto the child map.
     */
    @Test
    public void removeDeletesKeyFromChildMapAndBackMapsOldValue() {
        assertThat(this._mappedValues.remove(KEY1), is(nullValue()));
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        assertThat(this._mappedValues.remove(KEY1), is(EXTERNAL_VALUE1));
        assertThat(this._backingMap.containsKey(KEY1), is(false));
    }

    /**
     * Ensure size() passes calls onto the child map.
     */
    @Test
    public void sizePassesCallToChildMap() {
        assertThat(this._mappedValues.size(), is(0));
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        assertThat(this._mappedValues.size(), is(1));
    }

    /**
     * Ensure isEmpty() passes calls onto the child map.
     */
    @Test
    public void isEmptyPassesCallToChildMap() {
        assertThat(this._mappedValues.isEmpty(), is(true));
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        assertThat(this._mappedValues.isEmpty(), is(false));
    }

    /**
     * Ensure containsValue() maps external values to internal ones and passes calls onto the child map.
     */
    @Test
    public void containsValueMapsQueriedValue() {
        assertThat(this._mappedValues.containsValue(EXTERNAL_VALUE1), is(false));
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        assertThat(this._mappedValues.containsValue(EXTERNAL_VALUE1), is(true));
    }

    /**
     * Ensure putAll() maps external values to internal ones and passes calls onto the child map.
     */
    @Test
    public void putAllMapsAllValues() {
        this._mappedValues.putAll(EXTERNAL_VALUES_MAP);
        assertThat(this._backingMap, is(INTERNAL_VALUES_MAP));
    }

    /**
     * Ensure clear() passes calls onto the child map.
     */
    @Test
    public void clearPassesCallToChildMap() {
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        this._mappedValues.clear();
        assertThat(this._backingMap.isEmpty(), is(true));
    }

    /**
     * Ensure keySet() passes calls onto the child map.
     */
    @Test
    public void keySetPassesCallToChildMap() {
        assertThat(this._mappedValues.keySet(), is(Collections.emptySet()));
        this._backingMap.put(KEY1, INTERNAL_VALUE1);
        assertThat(this._mappedValues.keySet(), is(Collections.singleton(KEY1)));
    }

    /**
     * Ensure values() maps external values to internal ones and passes calls onto the child map.
     */
    @Test
    public void valuesBackMapsAllValues() {
        assertThat(this._mappedValues.values().isEmpty(), is(true));
        this._backingMap.putAll(
                INTERNAL_VALUES_MAP);
        assertThat(this._mappedValues.values(), is(
                List.of(EXTERNAL_VALUE1, EXTERNAL_VALUE2)));
    }

    /**
     * Ensure entrySet() maps external values to internal ones and passes calls onto the child map.
     */
    @Test
    public void entrySetBackMapsAllValues() {
        assertThat(this._mappedValues.entrySet().isEmpty(), is(true));
        this._backingMap.putAll(INTERNAL_VALUES_MAP);
        assertThat(this._mappedValues.entrySet(), is(
                Set.of(Pair.of(KEY1, EXTERNAL_VALUE1), Pair.of(KEY2, EXTERNAL_VALUE2))));
    }
}
