package org.kurin.routing;

public class KurinRoutingContext {
    private static final ThreadLocal<DataSourceType> contextHolder = new ThreadLocal<>();
    private static final ThreadLocal<String> shardHolder = new ThreadLocal<>();

    public static void set(DataSourceType dataSourceType) {
        contextHolder.set(dataSourceType);
    }

    public static DataSourceType get() {
        return contextHolder.get() != null ? contextHolder.get() : DataSourceType.PRIMARY; // PRIMARY за замовчуванням
    }

    /**
     * @return the explicitly selected type, or null if nothing was selected for the current thread
     */
    public static DataSourceType peek() {
        return contextHolder.get();
    }

    public static void clear() {
        contextHolder.remove();
    }

    public static void setShard(String shard) {
        shardHolder.set(shard);
    }

    public static String getShard() {
        return shardHolder.get();
    }

    public static void clearShard() {
        shardHolder.remove();
    }
}
