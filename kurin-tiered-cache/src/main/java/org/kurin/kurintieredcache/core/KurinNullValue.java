package org.kurin.kurintieredcache.core;

/**
 * Payload of an entry that caches "the source has no value for this key".
 */
public final class KurinNullValue {
    public static final KurinNullValue INSTANCE = new KurinNullValue();

    private KurinNullValue() {
    }

    @Override
    public String toString() {
        return "KurinNullValue";
    }
}
