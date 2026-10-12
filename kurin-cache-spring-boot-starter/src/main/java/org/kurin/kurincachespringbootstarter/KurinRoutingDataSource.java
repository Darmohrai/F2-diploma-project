package org.kurin.kurincachespringbootstarter;

import org.kurin.kurintieredcache.api.DataSourceType;
import org.kurin.kurintieredcache.api.KurinRoutingContext;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import java.util.HashMap;
import java.util.Map;

public class KurinRoutingDataSource extends AbstractRoutingDataSource {
    private final String singleShard;
    private final SessionStickyManager stickyManager;
    private final KurinClientIdResolver clientIdResolver;

    public KurinRoutingDataSource(KurinDataSourceTopology topology,
                                  SessionStickyManager stickyManager,
                                  KurinClientIdResolver clientIdResolver) {
        this.stickyManager = stickyManager;
        this.clientIdResolver = clientIdResolver;

        Map<Object, Object> targets = new HashMap<>();
        topology.shards().forEach((name, shard) -> {
            targets.put(lookupKey(name, DataSourceType.PRIMARY), shard.primary());
            targets.put(lookupKey(name, DataSourceType.REPLICA),
                    shard.replica() != null ? shard.replica() : shard.primary());
        });
        setTargetDataSources(targets);

        this.singleShard = topology.shards().size() == 1 ? topology.shards().keySet().iterator().next() : null;
    }

    @Override
    protected Object determineCurrentLookupKey() {
        String shard = KurinRoutingContext.getShard();
        if (shard == null) {
            shard = singleShard;
        }
        if (shard == null) {
            throw new IllegalStateException("No shard selected for the current operation. " +
                    "Mark the shard key argument of the method with @KurinShardKey.");
        }

        // A connection requested outside of @KurinReadOnly is treated as a write
        if (KurinRoutingContext.peek() == null) {
            stickyManager.markWriteForClient(clientIdResolver.resolveClientId());
        }

        return lookupKey(shard, KurinRoutingContext.get());
    }

    static String lookupKey(String shard, DataSourceType type) {
        return shard + "/" + type;
    }
}
