/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata.kafka;

import com.linkedin.kafka.cruisecontrol.persisteddata.MapDecorator;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.IExpectationSetters;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class KafkaPersistedMapTest {

    private static final String TOPIC = "topic";
    private static final int NUM_PARTITIONS = 1;
    private static final TopicPartition PARTITION_0 = new TopicPartition(TOPIC, 0);
    private static final CompletableFuture<RecordMetadata> SUCCESSFUL_PRODUCER_SEND_RESPONSE = CompletableFuture.completedFuture(
            new RecordMetadata(PARTITION_0, 1, 0, System.currentTimeMillis(), 0, 0));
    private static final Map<String, String> ADDITIONAL_CONFIGS = Map.of("max.message.bytes",
            "10000");
    private static final String KEY_1 = "key1";
    private static final String VALUE_1 = "value1";
    private static final String KEY_2 = "key2";
    private static final String VALUE_2 = "value2";
    private static final String KEY_3 = "key3";
    private static final String VALUE_3 = "value3";
    private static final long TEST_WAIT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

    @Mock
    private Producer<String, String> _mockProducer;
    @Mock
    private Consumer<String, String> _mockConsumer;

    private KeyQueueMapDecorator<String, String> _backingMap;
    private KafkaPersistedMap _persistedMap;

    private void initPersistedMap() {
        // Set up calls to the consumer in the background thread.
        setUpConsumerThread();

        // Persisted data is written to the backingMap.
        this._backingMap = new KeyQueueMapDecorator<>(new ConcurrentHashMap<>());

        // Initialize the object under test.
        this._persistedMap = new KafkaPersistedMap(this._backingMap, TOPIC, NUM_PARTITIONS,
                (short) 1, ADDITIONAL_CONFIGS, t -> {
        },
                () -> this._mockProducer, () -> this._mockConsumer);
    }

    private void setUpConsumerThread() {
        // Set up the consumer thread.
        this._mockConsumer.subscribe(eq(Set.of(TOPIC)));
        expectLastCall();
        this._mockConsumer.seekToBeginning(EasyMock.anyObject());
        expectLastCall();
        this._mockConsumer.close();
        expectLastCall();

        // Consumer returns no data for successive calls.
        expect(this._mockConsumer.poll(anyObject()))
                .andReturn(new ConsumerRecords<>(Collections.emptyMap())).anyTimes();
        replay(this._mockConsumer);
    }

    private void addConsumerData(Map<String, String> data) {
        expect(this._mockConsumer.poll(anyObject()))
                .andReturn(getConsumerRecordsForPartition(data)).times(1);
    }

    private static ConsumerRecords<String, String> getConsumerRecordsForPartition(
            Map<String, String> data) {
        final List<ConsumerRecord<String, String>> consumerRecords = data.entrySet().stream()
                .map(e -> new ConsumerRecord<>(PARTITION_0.topic(),
                        PARTITION_0.partition(),
                        1, e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return new ConsumerRecords<>(Map.of(PARTITION_0, consumerRecords));
    }

    private void expectProducerSuccess(String key, String value) {
        expectForProducer(key, value)
                .andReturn(SUCCESSFUL_PRODUCER_SEND_RESPONSE);
        replay(this._mockProducer);
    }

    private IExpectationSetters<Future<RecordMetadata>> expectForProducer(String key,
            String value) {
        return expect(this._mockProducer.send(eq(new ProducerRecord<>(TOPIC, key, value))));
    }

    private static class KeyQueueMapDecorator<K, V> extends MapDecorator<K, V> {

        // Maintains a queue of keys that were added to the map.
        private final BlockingQueue<K> _keyQueue;

        /**
         * Wraps the child map so accesses to it can be decorated.
         *
         * @param child The other map instance to decorate.
         */
        KeyQueueMapDecorator(Map<K, V> child) {
            super(child);
            this._keyQueue = new LinkedBlockingQueue<>();
        }

        @Override
        public V put(K key, V value) {
            // Store the value before notifying queue consumers.
            final V result = super.put(key, value);
            this._keyQueue.add(key);
            return result;
        }

        public K getKeyFromQueue() throws InterruptedException {
            return _keyQueue.poll(TEST_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Make sure all the config is applied and in the correct overriding order. Also make sure it is
     * passed to the clientFactory.
     */
    @Test
    public void createKafkaClientAppliesAllTheConfig() {
        final String bootstrapServers = "bootstrapServers";
        final Map<String, Object> defaultConfig = Map.of(
                KEY_1, VALUE_1,
                KEY_2, VALUE_1,
                KEY_3, VALUE_1);
        final Map<String, Object> securityConfig = Map.of(
                KEY_2, VALUE_2,
                KEY_3, VALUE_2);
        final Map<String, Object> additionalConfig = Map.of(
                KEY_3, VALUE_3);
        final Function<Map<String, Object>, Map<String, Object>> clientFactory =
                config -> {
                    config.put("called", true);
                    return config;
                };
        assertThat(KafkaPersistedMap.createKafkaClient(bootstrapServers,
                defaultConfig, securityConfig, additionalConfig, clientFactory), equalTo(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                KEY_1, VALUE_1,
                KEY_2, VALUE_2,
                KEY_3, VALUE_3,
                "called", true)));
    }

    /**
     * Ensure that closing the persisted map object, stops the background thread and closes the
     * kafka consumer.
     */
    @Test
    public void closeStopsTheConsumerThread() throws IOException, InterruptedException {
        // Consumer returns no data.
        initPersistedMap();

        // The background thread starts alive, then dies after close() is called.
        final Thread cacheUpdater = this._persistedMap.getCacheUpdater();
        assertTrue(cacheUpdater.isAlive());
        this._persistedMap.close();
        cacheUpdater.join(TEST_WAIT_TIMEOUT_MS);
        assertFalse(cacheUpdater.isAlive());
    }

    /**
     * All consumed data ends up in the backing map.
     */
    @Test
    public void backgroundThreadPutsConsumedDataIntoBackingMap() throws InterruptedException {
        // Consumer returns two records, then nothing.
        Map<String, String> data1 = Map.of(KEY_1, VALUE_1);
        Map<String, String> data2 = Map.of(KEY_2, VALUE_2);
        addConsumerData(data1);
        addConsumerData(data2);
        initPersistedMap();

        // Wait for the backing map to contain the data.
        assertThat(this._backingMap.getKeyFromQueue(), equalTo(KEY_1));
        assertThat(this._backingMap.getKeyFromQueue(), equalTo(KEY_2));

        // Make sure both keys and values have been inserted.
        assertThat(this._backingMap.get(KEY_1), equalTo(VALUE_1));
        assertThat(this._backingMap.get(KEY_2), equalTo(VALUE_2));
    }

    /**
     * All consumed data ends up in the backing map.
     */
    @Test
    public void backgroundThreadKeepsConsumingEvenAfterAnException() throws InterruptedException {
        // Consumer receives an exception then some valid data.
        Map<String, String> data1 = Map.of(KEY_1, VALUE_1);
        expect(this._mockConsumer.poll(anyObject()))
                .andThrow(new RuntimeException(
                        "For test: backgroundThreadKeepsConsumingEvenAfterAnException()")).times(1);
        addConsumerData(data1);
        initPersistedMap();

        // Wait for the backing map to contain the data.
        assertThat(this._backingMap.getKeyFromQueue(), equalTo(KEY_1));

        // Make sure keys and values have been inserted.
        assertThat(this._backingMap.get(KEY_1), equalTo(VALUE_1));
    }

    /**
     * The put method should return the previous value for a key.
     */
    @Test
    public void putReturnsPreviousValue() {
        initPersistedMap();
        expect(this._mockProducer.send(anyObject()))
                .andReturn(SUCCESSFUL_PRODUCER_SEND_RESPONSE)
                .times(2);
        replay(this._mockProducer);

        // Backing map is initially empty so null value should be returned.
        assertThat(this._persistedMap.put(KEY_1, VALUE_1), equalTo(null));

        // Manually place a value into the backing store, then see if it is returned on the next put.
        this._backingMap.put(KEY_1, VALUE_1);
        assertThat(this._persistedMap.put(KEY_1, VALUE_2), equalTo(VALUE_1));
    }

    /**
     * Put produces the data to kafka.
     */
    @Test
    public void putGivesDataToProducer() {
        initPersistedMap();
        expectProducerSuccess(KEY_1, VALUE_1);

        // This test will fail if the above mock call is not used.
        this._persistedMap.put(KEY_1, VALUE_1);
        verify(this._mockProducer);
    }

    /**
     * Put throws an exception if persisting the data fails.
     */
    @Test(expected = KafkaPersistedMapException.class)
    public void putHandlesFailedProduce() {
        initPersistedMap();
        expectForProducer(KEY_1, VALUE_1)
                .andReturn(CompletableFuture.failedFuture(
                        new ExecutionException("test exception", null)));
        replay(this._mockProducer);

        try {
            this._persistedMap.put(KEY_1, VALUE_1);
        } finally {
            // This test will fail if the above mock call is not used.
            verify(this._mockProducer);
        }
    }
}
