/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(EasyMockRunner.class)
public class MapDecoratorTest {

    private static final Integer KEY = 1;
    private static final String VALUE = "value";

    @Mock
    private Map<Integer, String> _mockChild;

    private Map<Integer, String> _decorator;

    /**
     * Common setup for each test.
     */
    @Before
    public void beforeTest() {
        this._decorator = new MapDecorator<>(this._mockChild) {
        };
    }

    /**
     * Passes the call to get() on the decorated child map.
     */
    @Test
    public void getDecoratesChildMap() {
        expect(this._mockChild.get(eq(KEY))).andReturn(VALUE);
        replay(this._mockChild);
        assertThat(this._decorator.get(KEY), is(VALUE));
        verify(this._mockChild);
    }

    /**
     * Passes the call to put() on the decorated child map.
     */
    @Test
    public void putDecoratesChildMap() {
        expect(this._mockChild.put(eq(KEY), eq(VALUE))).andReturn(null);
        replay(this._mockChild);
        assertThat(this._decorator.put(KEY, VALUE), is(nullValue()));
        verify(this._mockChild);
    }

    /**
     * Passes the call to size() on the decorated child map.
     */
    @Test
    public void sizeDecoratesChildMap() {
        expect(this._mockChild.size()).andReturn(1);
        replay(this._mockChild);
        assertThat(this._decorator.size(), is(1));
        verify(this._mockChild);
    }

    /**
     * Passes the call to isEmpty() on the decorated child map.
     */
    @Test
    public void isEmptyDecoratesChildMap() {
        expect(this._mockChild.isEmpty()).andReturn(true);
        replay(this._mockChild);
        assertThat(this._decorator.isEmpty(), is(true));
        verify(this._mockChild);
    }

    /**
     * Passes the call to containsKey() on the decorated child map.
     */
    @Test
    public void containsKeyDecoratesChildMap() {
        expect(this._mockChild.containsKey(eq(KEY))).andReturn(true);
        replay(this._mockChild);
        assertThat(this._decorator.containsKey(KEY), is(true));
        verify(this._mockChild);
    }

    /**
     * Passes the call to containsValue() on the decorated child map.
     */
    @Test
    public void containsValueDecoratesChildMap() {
        expect(this._mockChild.containsValue(eq(VALUE))).andReturn(true);
        replay(this._mockChild);
        assertThat(this._decorator.containsValue(VALUE), is(true));
        verify(this._mockChild);
    }

    /**
     * Passes the call to remove() on the decorated child map.
     */
    @Test
    public void removeDecoratesChildMap() {
        expect(this._mockChild.remove(eq(KEY))).andReturn(VALUE);
        replay(this._mockChild);
        assertThat(this._decorator.remove(KEY), is(VALUE));
        verify(this._mockChild);
    }

    /**
     * Passes the call to putAll() on the decorated child map.
     */
    @Test
    public void putAllDecoratesChildMap() {
        final Map<Integer, String> data = Map.of(KEY, VALUE);
        this._mockChild.putAll(eq(data));
        expectLastCall();
        replay(this._mockChild);
        this._decorator.putAll(data);
        verify(this._mockChild);
    }

    /**
     * Passes the call to clear() on the decorated child map.
     */
    @Test
    public void clearDecoratesChildMap() {
        this._mockChild.clear();
        expectLastCall();
        replay(this._mockChild);
        this._decorator.clear();
        verify(this._mockChild);
    }

    /**
     * Passes the call to keySet() on the decorated child map.
     */
    @Test
    public void keySetDecoratesChildMap() {
        Set<Integer> keys = Collections.singleton(KEY);
        expect(this._mockChild.keySet()).andReturn(keys);
        replay(this._mockChild);
        assertThat(this._decorator.keySet(), is(keys));
        verify(this._mockChild);
    }

    /**
     * Passes the call to values() on the decorated child map.
     */
    @Test
    public void valuesDecoratesChildMap() {
        Set<String> values = Collections.singleton(VALUE);
        expect(this._mockChild.values()).andReturn(values);
        replay(this._mockChild);
        assertThat(this._decorator.values(), is(values));
        verify(this._mockChild);
    }

    /**
     * Passes the call to entrySet() on the decorated child map.
     */
    @Test
    public void entrySetDecoratesChildMap() {
        Set<Map.Entry<Integer, String>> entries = Collections.singleton(Pair.of(KEY, VALUE));
        expect(this._mockChild.entrySet()).andReturn(entries);
        replay(this._mockChild);
        assertThat(this._decorator.entrySet(), is(entries));
        verify(this._mockChild);
    }
}
