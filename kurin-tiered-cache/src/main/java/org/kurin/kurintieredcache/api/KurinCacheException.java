package org.kurin.kurintieredcache.api;

import org.kurin.api.exception.KurinException;

/**
 * A cache operation was used incorrectly or could not be carried out.
 */
public class KurinCacheException extends KurinException {
    public KurinCacheException(String message) {
        super(message);
    }

    public KurinCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
