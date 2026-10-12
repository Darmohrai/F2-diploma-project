package org.kurin.kurintieredcache.core;

import java.io.Serializable;

public class KurinNullValue implements Serializable {
    public static final KurinNullValue INSTANCE = new KurinNullValue();

    private KurinNullValue() {
    }
}
