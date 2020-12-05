package edu.auth.jetproud.proud.distributables;

import com.hazelcast.function.SupplierEx;

import java.io.Serializable;
import java.util.HashMap;

public class KeyedStateHolder<K extends Serializable, V extends Serializable> extends HashMap<K, V> implements Serializable
{

    public KeyedStateHolder() {
        super();
    }

    public V get(K key) {
        return super.get(key);
    }

    public V getOrDefault(K key, V defaultValue) {
        return super.getOrDefault(key, defaultValue);
    }

    public V put(K key, V value) {
        return super.put(key, value);
    }


    public static <K extends Serializable, V extends Serializable> KeyedStateHolder<K,V> create() {
        return new KeyedStateHolder<>();
    }

    public static <K extends Serializable, V extends Serializable> SupplierEx<KeyedStateHolder<K,V>> supplier() {
        return () -> new KeyedStateHolder<>();
    }

}
