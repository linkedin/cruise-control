/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Map} where the values stored internally are a different type or format than those that
 * are externally used. For example, a number can be stored, but internally it is stored as a
 * string. The functions that do the mapping between the two representations just need to be 1:1
 * inverses of each other.
 */
public class ValueMappedMap<K, EXTERNALVALUE, INTERNALVALUE> implements Map<K, EXTERNALVALUE> {

    private static final Logger LOG = LoggerFactory.getLogger(ValueMappedMap.class);

    // Stores the actual internal data.
    private final Map<K, INTERNALVALUE> _child;

    // Converts an external value to an internal value.
    private final Function<Object, INTERNALVALUE> _valueMapper;

    // Converts an internal value back to an external value.
    private final Function<INTERNALVALUE, EXTERNALVALUE> _valueBackMapper;

    /**
     * Wraps all value reference methods of the child map by first applying the
     * {@code valueMapper} to the value.
     *
     * @param child The map that stores all the actual data.
     * @param valueMapper Used to remap incoming values to the internally stored values.
     * @param valueBackMapper Used to remap outgoing values from the internally stored values.
     */
    public ValueMappedMap(@Nonnull Map<K, INTERNALVALUE> child,
            Function<Object, INTERNALVALUE> valueMapper,
            Function<INTERNALVALUE, EXTERNALVALUE> valueBackMapper) {
        this._child = child;
        this._valueMapper = valueMapper;
        this._valueBackMapper = valueBackMapper;
    }

    @Override
    public EXTERNALVALUE get(Object key) {
        final INTERNALVALUE internalValue = this._child.get(key);
        if (internalValue == null) {
            return null;
        }
        final EXTERNALVALUE externalValue = this._valueBackMapper.apply(internalValue);
        LOG.debug("Getting key={}, internalValue={}, externalValue={}",
                key, internalValue, externalValue);
        return externalValue;
    }

    @Override
    public EXTERNALVALUE put(K key, EXTERNALVALUE externalValue) {
        final INTERNALVALUE internalValue = this._valueMapper.apply(externalValue);
        final INTERNALVALUE previousInternalValue = this._child.put(key, internalValue);
        LOG.debug("Putting key={}, externalValue={}, internalValue={}, previousInternalValue={}",
                key, externalValue, internalValue, previousInternalValue);
        if (previousInternalValue == null) {
            return null;
        }
        return this._valueBackMapper.apply(previousInternalValue);
    }

    @Override
    public boolean containsKey(Object key) {
        return this._child.containsKey(key);
    }

    @Override
    public EXTERNALVALUE remove(Object key) {
        final INTERNALVALUE internalvalue = this._child.remove(key);
        if (internalvalue == null) {
            return null;
        }
        return this._valueBackMapper.apply(internalvalue);
    }

    @Override
    public int size() {
        return this._child.size();
    }

    @Override
    public boolean isEmpty() {
        return this._child.isEmpty();
    }

    @Override
    public boolean containsValue(Object value) {
        return this._child.containsValue(this._valueMapper.apply(value));
    }

    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends EXTERNALVALUE> map) {
        final Map<K, ? extends INTERNALVALUE> remapped =
                map.entrySet().stream().collect(
                        Collectors.toMap(Entry::getKey,
                                e -> this._valueMapper.apply(e.getValue()))
                );
        this._child.putAll(remapped);
    }

    @Override
    public void clear() {
        this._child.clear();
    }

    @Override
    public Set<K> keySet() {
        return this._child.keySet();
    }

    @Override
    public Collection<EXTERNALVALUE> values() {
        return this._child.values().stream().map(this._valueBackMapper)
                .collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, EXTERNALVALUE>> entrySet() {
        return this._child.entrySet().stream()
                .map(e -> Pair.of(e.getKey(), this._valueBackMapper.apply(e.getValue())))
                .collect(Collectors.toSet());
    }
}
