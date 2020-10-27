package edu.auth.jetproud.proud.distributables;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.replicatedmap.ReplicatedMap;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DistributedReplicatedMap<K,V> extends AnyDistributedObject<ReplicatedMap<K,V>> implements ReplicatedMap<K,V>, Serializable
{
    public DistributedReplicatedMap(String name) {
        super(name);
    }

    @Override
    protected ReplicatedMap<K, V> getData(HazelcastInstance hazelcastInstance, String name) {
        return hazelcastInstance.getReplicatedMap(name);
    }

    // Proxy Impl


    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        return getData().put(key, value, ttl, timeUnit);
    }

    @Override
    public void clear() {
        getData().clear();
    }

    @Override
    public boolean removeEntryListener(UUID id) {
        return getData().removeEntryListener(id);
    }

    @Override
    public UUID addEntryListener(EntryListener<K, V> listener) {
        return getData().addEntryListener(listener);
    }

    @Override
    public UUID addEntryListener(EntryListener<K, V> listener, K key) {
        return getData().addEntryListener(listener, key);
    }

    @Override
    public UUID addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        return getData().addEntryListener(listener, predicate);
    }

    @Override
    public UUID addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        return getData().addEntryListener(listener, predicate, key);
    }

    @Override
    public Collection<V> values() {
        return getData().values();
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        return getData().values(comparator);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return getData().entrySet();
    }

    @Override
    public Set<K> keySet() {
        return getData().keySet();
    }

    @Override
    public LocalReplicatedMapStats getReplicatedMapStats() {
        return getData().getReplicatedMapStats();
    }

    @Override
    public int size() {
        return getData().size();
    }

    @Override
    public boolean isEmpty() {
        return getData().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return getData().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return getData().containsValue(value);
    }

    @Override
    public V get(Object key) {
        return getData().get(key);
    }

    @Override
    public V put(K key, V value) {
        return getData().put(key, value);
    }

    @Override
    public V remove(Object key) {
        return getData().remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        getData().putAll(m);
    }

    @Override
    public boolean equals(Object o) {
        return getData().equals(o);
    }

    @Override
    public int hashCode() {
        return getData().hashCode();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return getData().getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        getData().forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        getData().replaceAll(function);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return getData().putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return getData().remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return getData().replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        return getData().replace(key, value);
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return getData().computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return getData().computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return getData().compute(key, remappingFunction);
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return getData().merge(key, value, remappingFunction);
    }

    @Override
    public String getPartitionKey() {
        return getData().getPartitionKey();
    }

    @Override
    public String getName() {
        return getData().getName();
    }

    @Override
    public String getServiceName() {
        return getData().getServiceName();
    }

    @Override
    public void destroy() {
        getData().destroy();
    }
}
