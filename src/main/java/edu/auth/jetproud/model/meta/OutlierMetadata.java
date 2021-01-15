package edu.auth.jetproud.model.meta;

import edu.auth.jetproud.model.AnyProudData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OutlierMetadata<T extends AnyProudData> extends HashMap<Integer,T> implements Serializable
{

    public OutlierMetadata() {
        super();
    }

    public OutlierMetadata(Map<? extends Integer, ? extends T> m) {
        super(m);
    }
}
