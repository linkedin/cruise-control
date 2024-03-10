/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyMappedMapTest {

    private static final Integer EXTERNAL_KEY1 = 1;
    private static final Integer EXTERNAL_KEY2 = 3;
    private static final Integer INTERNAL_KEY1 = 2;
    private static final Integer INTERNAL_KEY2 = 6;
    private static final Integer VALUE1 = 11;
    private static final Integer VALUE2 = 22;
    private static final Map<Integer, Integer> EXTERNAL_KEYS_MAP =
            Map.of(EXTERNAL_KEY1, VALUE1, EXTERNAL_KEY2, VALUE2);
    private static final Map<Integer, Integer> INTERNAL_KEYS_MAP =
            Map.of(INTERNAL_KEY1, VALUE1, INTERNAL_KEY2, VALUE2);

    private Map<Integer, Integer> _backingMap;
    private Map<Integer, Integer> _mappedValues;

    /**
     * Set a common starting point.
     */
    @Before
    public void beforeTest() {
        this._backingMap = new HashMap<>();
        this._mappedValues = new KeyMappedMap<>(this._backingMap,
                KeyMappedMapTest::doubleInteger, KeyMappedMapTest::halveInteger);
    }

    // The key mapper function.
    private static Integer doubleInteger(Object o) {
        if (!(o instanceof Integer)) {
            throw new IllegalArgumentException(
                    String.format("Key must be an Integer but was: %s",
                            o.getClass().getSimpleName()));
        }
        return (Integer) o * 2;
    }

    // The key back mapper function.
    private static Integer halveInteger(Integer i) {
        return i % 2 == 0 ? i / 2 : null;
    }

    /**
     * Ensure get() maps external key values to internal ones and passes calls to the child map.
     */
    @Test
    public void getAppliesMapperToKey() {
        assertThat(this._mappedValues.get(EXTERNAL_KEY1), is(nullValue()));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        assertThat(this._mappedValues.get(EXTERNAL_KEY1), is(VALUE1));
    }

    /**
     * Ensure put() maps external key values to internal ones and passes calls to the child map.
     */
    @Test
    public void putAppliesMapperToKey() {
        assertThat(this._mappedValues.put(EXTERNAL_KEY1, VALUE1), is(nullValue()));
        assertThat(this._backingMap.get(INTERNAL_KEY1), is(VALUE1));
        assertThat(this._mappedValues.put(EXTERNAL_KEY1, VALUE1), is(VALUE1));
    }

    /**
     * Ensure containsKey() maps external key values to internal ones and passes calls to the child map.
     */
    @Test
    public void containsKeyAppliesMapperToKey() {
        assertThat(this._mappedValues.containsKey(EXTERNAL_KEY1), is(false));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        assertThat(this._mappedValues.containsKey(EXTERNAL_KEY1), is(true));
    }

    /**
     * Ensure remove() maps external key values to internal ones and passes calls to the child map.
     */
    @Test
    public void removeAppliesMapperToKey() {
        assertThat(this._mappedValues.remove(EXTERNAL_KEY1), is(nullValue()));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        assertThat(this._mappedValues.remove(EXTERNAL_KEY1), is(VALUE1));
    }

    /**
     * Ensure size() passes calls to the child map.
     */
    @Test
    public void sizePassesCallToChildMap() {
        assertThat(this._mappedValues.size(), is(0));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        assertThat(this._mappedValues.size(), is(1));
    }

    /**
     * Ensure size() only counts entries where the key works with the mapper and ignores the others.
     */
    @Test
    public void sizeOnlyIncludesMappableKeys() {
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        this._backingMap.put(EXTERNAL_KEY1, VALUE1);
        assertThat(this._mappedValues.size(), is(1));
    }

    /**
     * Ensure isEmpty() passes calls to the child map.
     */
    @Test
    public void isEmptyPassesCallToChildMap() {
        assertThat(this._mappedValues.isEmpty(), is(true));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        assertThat(this._mappedValues.isEmpty(), is(false));
    }

    /**
     * Ensure containsValue() finds values belonging to the used keyMapper and ignores other mapped
     * keys.
     */
    @Test
    public void containsValueFindsKeyMappedKeyValues() {
        assertThat(this._mappedValues.containsValue(VALUE1), is(false));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        this._backingMap.put(EXTERNAL_KEY2, VALUE2);
        assertThat(this._mappedValues.containsValue(VALUE1), is(true));
        assertThat(this._mappedValues.containsValue(VALUE2), is(false));
    }

    /**
     * Ensure putAll() maps external key values to internal ones and passes them to the child map.
     */
    @Test
    public void putAllMapsAllKeys() {
        this._mappedValues.putAll(EXTERNAL_KEYS_MAP);
        assertThat(this._backingMap, is(INTERNAL_KEYS_MAP));
    }

    /**
     * Ensure clear() removes all entries with keyMapped keys.
     */
    @Test
    public void clearRemovesKeyMappedKeys() {
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        this._backingMap.put(EXTERNAL_KEY2, VALUE2);
        this._mappedValues.clear();
        assertThat(this._backingMap.containsKey(INTERNAL_KEY1), is(false));
        assertThat(this._backingMap.containsKey(EXTERNAL_KEY2), is(true));
    }

    /**
     * Ensure keySet() maps external keys to internal ones and only including those that are
     * keyMappable.
     */
    @Test
    public void keySetMapsAllKeys() {
        assertThat(this._mappedValues.keySet().isEmpty(), is(true));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        this._backingMap.put(EXTERNAL_KEY2, VALUE2);
        assertThat(this._mappedValues.keySet(), is(Set.of(EXTERNAL_KEY1)));
    }

    /**
     * Ensure values() returns only values of key mapped entries.
     */
    @Test
    public void valuesReturnsValuesOfKeyMappedEntries() {
        assertThat(this._mappedValues.values().isEmpty(), is(true));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        this._backingMap.put(EXTERNAL_KEY2, VALUE2);
        assertThat(this._mappedValues.values(), allOf(hasItems(VALUE1), not(hasItems(VALUE2))));
    }

    /**
     * Ensure entrySet() includes all keyMapped entries.
     */
    @Test
    public void entrySetMapsAllKeys() {
        assertThat(this._mappedValues.entrySet().isEmpty(), is(true));
        this._backingMap.put(INTERNAL_KEY1, VALUE1);
        this._backingMap.put(EXTERNAL_KEY2, VALUE2);
        assertThat(this._mappedValues.entrySet(), is(Set.of(Pair.of(EXTERNAL_KEY1, VALUE1))));
    }
}
