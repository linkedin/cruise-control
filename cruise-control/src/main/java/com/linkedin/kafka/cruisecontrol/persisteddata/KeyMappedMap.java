/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Map} where the keys stored internally are a different type or format than those that are
 * externally used. For example, a number can be stored, but internally it is stored as a string.
 * The functions that do the mapping between the two representations just need to be 1:1 inverses of
 * each other.
 */
public class KeyMappedMap<EXTERNALKEY, INTERNALKEY, V> implements Map<EXTERNALKEY, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyMappedMap.class);

    // Stores the actual internal data.
    private final Map<INTERNALKEY, V> _child;

    // Converts an external key to an internal key.
    private final Function<Object, INTERNALKEY> _keyMapper;

    // Converts an internal key back to an external key.
    private final Function<INTERNALKEY, EXTERNALKEY> _keyBackMapper;

    /**
     * Wraps all key reference methods of the child map by first applying the
     * <code>keyMapper</code> to the key.
     *
     * @param child The map that stores all the actual data.
     * @param keyMapper Used to remap incoming keys to the internally stored keys.
     * @param keyBackMapper Used to remap outgoing keys from the internally stored keys. To ensure
     * keys are correctly mapped back, they should validate that the key is coming from the correct
     * name/number space, and return null if the validation fails. For example, for a String mapper
     * that adds a prefix string to each key, the back mapper should return null if that same prefix
     * is not present on the mapped key.
     */
    public KeyMappedMap(@Nonnull Map<INTERNALKEY, V> child,
            Function<Object, INTERNALKEY> keyMapper,
            Function<INTERNALKEY, EXTERNALKEY> keyBackMapper) {
        this._child = child;
        this._keyMapper = keyMapper;
        this._keyBackMapper = keyBackMapper;
    }

    @Override
    public V get(Object externalKey) {
        final INTERNALKEY internalKey = this._keyMapper.apply(externalKey);
        final V value = this._child.get(internalKey);
        LOG.debug("Getting externalKey={}, internalKey={} value={}",
                externalKey, internalKey, value);
        return value;
    }

    @Override
    public V put(EXTERNALKEY externalKey, V value) {
        final INTERNALKEY internalKey = this._keyMapper.apply(externalKey);
        LOG.debug("Putting externalKey={}, internalKey={} value={}",
                externalKey, internalKey, value);
        return this._child.put(internalKey, value);
    }

    @Override
    public boolean containsKey(Object key) {
        return this._child.containsKey(this._keyMapper.apply(key));
    }

    @Override
    public V remove(Object key) {
        return this._child.remove(this._keyMapper.apply(key));
    }

    @Override
    public int size() {
        return this.keySet().size();
    }

    @Override
    public boolean isEmpty() {
        return this.keySet().isEmpty();
    }

    @Override
    public boolean containsValue(Object value) {
        return this._child.entrySet().stream()
                .filter(e -> Objects.nonNull(this._keyBackMapper.apply(e.getKey())))
                .map(Entry::getValue)
                .anyMatch(v -> Objects.equals(v, value));
    }

    @Override
    public void putAll(@Nonnull Map<? extends EXTERNALKEY, ? extends V> map) {
        this._child.putAll(
                map.entrySet().stream().collect(
                        Collectors.toMap(e -> this._keyMapper.apply(e.getKey()), Entry::getValue)
                ));
    }

    @Override
    public void clear() {
        this.keySet().stream()
                .map(this._keyMapper)
                .forEach(this._child::remove);
    }

    @Override
    public Set<EXTERNALKEY> keySet() {
        return this._child.keySet().stream()
                .map(this._keyBackMapper)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        return this._child.entrySet().stream()
                .filter(e -> Objects.nonNull(this._keyBackMapper.apply(e.getKey())))
                .map(Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public Set<Entry<EXTERNALKEY, V>> entrySet() {
        return this._child.entrySet().stream()
                .map(e -> Pair.of(this._keyBackMapper.apply(e.getKey()), e.getValue()))
                .filter(e -> Objects.nonNull(e.getKey()))
                .collect(Collectors.toSet());
    }
}
