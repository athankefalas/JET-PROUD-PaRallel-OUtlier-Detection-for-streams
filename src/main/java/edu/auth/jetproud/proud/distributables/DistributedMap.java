package edu.auth.jetproud.proud.distributables;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.*;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DistributedMap<K,V> extends AnyDistributedObject<IMap<K,V>> implements IMap<K,V>, Serializable
{
    public DistributedMap(String name) {
        super(name);
    }

    @Override
    protected IMap<K, V> getData(HazelcastInstance hazelcastInstance, String name) {
        return hazelcastInstance.getMap(name);
    }

    // Proxy Impl

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        getData().putAll(m);
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
    public boolean remove(Object key, Object value) {
        return getData().remove(key, value);
    }

    @Override
    public void removeAll(Predicate<K, V> predicate) {
        getData().removeAll(predicate);
    }

    @Override
    public void delete(Object key) {
        getData().delete(key);
    }

    @Override
    public void flush() {
        getData().flush();
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return getData().getAll(keys);
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        getData().loadAll(replaceExistingValues);
    }

    @Override
    public void loadAll(Set<K> keys, boolean replaceExistingValues) {
        getData().loadAll(keys, replaceExistingValues);
    }

    @Override
    public void clear() {
        getData().clear();
    }

    @Override
    public CompletionStage<V> getAsync(K key) {
        return getData().getAsync(key);
    }

    @Override
    public CompletionStage<V> putAsync(K key, V value) {
        return getData().putAsync(key, value);
    }

    @Override
    public CompletionStage<V> putAsync(K key, V value, long ttl, TimeUnit ttlUnit) {
        return getData().putAsync(key, value, ttl, ttlUnit);
    }

    @Override
    public CompletionStage<V> putAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        return getData().putAsync(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public CompletionStage<Void> putAllAsync(Map<? extends K, ? extends V> map) {
        return getData().putAllAsync(map);
    }

    @Override
    public CompletionStage<Void> setAsync(K key, V value) {
        return getData().setAsync(key, value);
    }

    @Override
    public CompletionStage<Void> setAsync(K key, V value, long ttl, TimeUnit ttlUnit) {
        return getData().setAsync(key, value, ttl, ttlUnit);
    }

    @Override
    public CompletionStage<Void> setAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        return getData().setAsync(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public CompletionStage<V> removeAsync(K key) {
        return getData().removeAsync(key);
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        return getData().tryRemove(key, timeout, timeunit);
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        return getData().tryPut(key, value, timeout, timeunit);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit ttlUnit) {
        return getData().put(key, value, ttl, ttlUnit);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        return getData().put(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit ttlUnit) {
        getData().putTransient(key, value, ttl, ttlUnit);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        getData().putTransient(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return getData().putIfAbsent(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit) {
        return getData().putIfAbsent(key, value, ttl, ttlUnit);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        return getData().putIfAbsent(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
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
    public void set(K key, V value) {
        getData().set(key, value);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit ttlUnit) {
        getData().set(key, value, ttl, ttlUnit);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        getData().set(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public void lock(K key) {
        getData().lock(key);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        getData().lock(key, leaseTime, timeUnit);
    }

    @Override
    public boolean isLocked(K key) {
        return getData().isLocked(key);
    }

    @Override
    public boolean tryLock(K key) {
        return getData().tryLock(key);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        return getData().tryLock(key, time, timeunit);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseTimeunit) throws InterruptedException {
        return getData().tryLock(key, time, timeunit, leaseTime, leaseTimeunit);
    }

    @Override
    public void unlock(K key) {
        getData().unlock(key);
    }

    @Override
    public void forceUnlock(K key) {
        getData().forceUnlock(key);
    }

    @Override
    public UUID addLocalEntryListener(MapListener listener) {
        return getData().addLocalEntryListener(listener);
    }

    @Override
    public UUID addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return getData().addLocalEntryListener(listener, predicate, includeValue);
    }

    @Override
    public UUID addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return getData().addLocalEntryListener(listener, predicate, key, includeValue);
    }

    @Override
    public String addInterceptor(MapInterceptor interceptor) {
        return getData().addInterceptor(interceptor);
    }

    @Override
    public boolean removeInterceptor(String id) {
        return getData().removeInterceptor(id);
    }

    @Override
    public UUID addEntryListener(MapListener listener, boolean includeValue) {
        return getData().addEntryListener(listener, includeValue);
    }

    @Override
    public boolean removeEntryListener(UUID id) {
        return getData().removeEntryListener(id);
    }

    @Override
    public UUID addPartitionLostListener(MapPartitionLostListener listener) {
        return getData().addPartitionLostListener(listener);
    }

    @Override
    public boolean removePartitionLostListener(UUID id) {
        return getData().removePartitionLostListener(id);
    }

    @Override
    public UUID addEntryListener(MapListener listener, K key, boolean includeValue) {
        return getData().addEntryListener(listener, key, includeValue);
    }

    @Override
    public UUID addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return getData().addEntryListener(listener, predicate, includeValue);
    }

    @Override
    public UUID addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        return getData().addEntryListener(listener, predicate, key, includeValue);
    }

    @Override
    public EntryView<K, V> getEntryView(K key) {
        return getData().getEntryView(key);
    }

    @Override
    public boolean evict(K key) {
        return getData().evict(key);
    }

    @Override
    public void evictAll() {
        getData().evictAll();
    }

    @Override
    public Set<K> keySet() {
        return getData().keySet();
    }

    @Override
    public Collection<V> values() {
        return getData().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return getData().entrySet();
    }

    @Override
    public Set<K> keySet(Predicate<K, V> predicate) {
        return getData().keySet(predicate);
    }

    @Override
    public Set<Entry<K, V>> entrySet(Predicate<K, V> predicate) {
        return getData().entrySet(predicate);
    }

    @Override
    public Collection<V> values(Predicate<K, V> predicate) {
        return getData().values(predicate);
    }

    @Override
    public Set<K> localKeySet() {
        return getData().localKeySet();
    }

    @Override
    public Set<K> localKeySet(Predicate<K, V> predicate) {
        return getData().localKeySet(predicate);
    }

    @Override
    public void addIndex(IndexType type, String... attributes) {
        getData().addIndex(type, attributes);
    }

    @Override
    public void addIndex(IndexConfig indexConfig) {
        getData().addIndex(indexConfig);
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return getData().getLocalMapStats();
    }

    @Override
    public <R> R executeOnKey(K key, EntryProcessor<K, V, R> entryProcessor) {
        return getData().executeOnKey(key, entryProcessor);
    }

    @Override
    public <R> Map<K, R> executeOnKeys(Set<K> keys, EntryProcessor<K, V, R> entryProcessor) {
        return getData().executeOnKeys(keys, entryProcessor);
    }

    @Override
    public <R> CompletionStage<Map<K, R>> submitToKeys(Set<K> keys, EntryProcessor<K, V, R> entryProcessor) {
        return getData().submitToKeys(keys, entryProcessor);
    }

    @Override
    public <R> CompletionStage<R> submitToKey(K key, EntryProcessor<K, V, R> entryProcessor) {
        return getData().submitToKey(key, entryProcessor);
    }

    @Override
    public <R> Map<K, R> executeOnEntries(EntryProcessor<K, V, R> entryProcessor) {
        return getData().executeOnEntries(entryProcessor);
    }

    @Override
    public <R> Map<K, R> executeOnEntries(EntryProcessor<K, V, R> entryProcessor, Predicate<K, V> predicate) {
        return getData().executeOnEntries(entryProcessor, predicate);
    }

    @Override
    public <R> R aggregate(Aggregator<? super Entry<K, V>, R> aggregator) {
        return getData().aggregate(aggregator);
    }

    @Override
    public <R> R aggregate(Aggregator<? super Entry<K, V>, R> aggregator, Predicate<K, V> predicate) {
        return getData().aggregate(aggregator, predicate);
    }

    @Override
    public <R> Collection<R> project(Projection<? super Entry<K, V>, R> projection) {
        return getData().project(projection);
    }

    @Override
    public <R> Collection<R> project(Projection<? super Entry<K, V>, R> projection, Predicate<K, V> predicate) {
        return getData().project(projection, predicate);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name) {
        return getData().getQueryCache(name);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name, Predicate<K, V> predicate, boolean includeValue) {
        return getData().getQueryCache(name, predicate, includeValue);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name, MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        return getData().getQueryCache(name, listener, predicate, includeValue);
    }

    @Override
    public boolean setTtl(K key, long ttl, TimeUnit timeunit) {
        return getData().setTtl(key, ttl, timeunit);
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
    public int size() {
        return getData().size();
    }

    @Override
    public boolean isEmpty() {
        return getData().isEmpty();
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
