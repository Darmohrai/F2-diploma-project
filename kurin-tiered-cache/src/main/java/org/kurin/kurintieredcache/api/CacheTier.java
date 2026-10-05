package org.kurin.kurintieredcache.api;

public enum CacheTier {
    L1_MEMORY,

    L2_ROCKSDB,

    L3_EXTERNAL,

    TIERED_SMART
}
