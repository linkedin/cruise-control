/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Decorates a standard {@link Map} allowing a subset of methods to be overridden instead of all of
 * them.
 *
 * @param <K> The map key type.
 * @param <V> The map value type.
 */
public abstract class MapDecorator<K, V> implements Map<K, V> {

    protected Map<K, V> _child;

    /**
     * Wraps the child map so accesses to it can be decorated.
     *
     * @param child The other map instance to decorate.
     */
    public MapDecorator(Map<K, V> child) {
        this._child = child;
    }

    @Override
    public V get(Object key) {
        return this._child.get(key);
    }

    @Override
    public V put(K key, V value) {
        return this._child.put(key, value);
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
    public boolean containsKey(Object key) {
        return this._child.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return this._child.containsValue(value);
    }

    @Override
    public V remove(Object key) {
        return this._child.remove(key);
    }

    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends V> map) {
        this._child.putAll(map);
    }

    @Override
    public void clear() {
        this._child.clear();
    }

    @Nonnull
    @Override
    public Set<K> keySet() {
        return this._child.keySet();
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        return this._child.values();
    }

    @Nonnull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return this._child.entrySet();
    }
}
