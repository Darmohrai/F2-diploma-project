package org.kurin.routing.spring.migration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Runs the Flyway migrations of the application on the primary of every shard, one shard after another.
 * <p>
 * It stands in for a {@link Flyway} that was configured for the routing data source. For each shard a Flyway
 * with the same configuration and the primary of the shard as its data source is created and run, so every
 * setting the application made applies to every shard as it is. Replicas are not touched.
 * <p>
 * Only {@link #migrate()} is spread over the shards. The other operations of Flyway (info, validate, repair,
 * clean) still address the routing data source; run them per shard through {@link #forShard(String)}.
 */
public class KurinShardedFlyway extends Flyway {
    private static final Logger log = LoggerFactory.getLogger(KurinShardedFlyway.class);

    private final Map<String, DataSource> primaries;

    /**
     * @param configuration the configuration of the Flyway of the application
     * @param primaries     the primaries to migrate by shard name, in order
     */
    public KurinShardedFlyway(Configuration configuration, Map<String, DataSource> primaries) {
        super(configuration);
        this.primaries = primaries;
    }

    /**
     * @return the result of the last shard; a failure on any shard stops the run and names the shard
     */
    @Override
    public MigrateResult migrate() throws FlywayException {
        MigrateResult last = null;
        for (String shard : primaries.keySet()) {
            log.info("Running Flyway migrations on the primary of shard '{}'", shard);
            try {
                last = forShard(shard).migrate();
            } catch (RuntimeException e) {
                throw new FlywayException("Flyway migration of shard '" + shard + "' failed: " + e.getMessage(), e);
            }
        }
        return last;
    }

    /**
     * @return a Flyway with the configuration of the application that works on the primary of the given shard
     * @throws IllegalArgumentException if the shard is not one this object migrates
     */
    public Flyway forShard(String shard) {
        DataSource primary = primaries.get(shard);
        if (primary == null) {
            throw new IllegalArgumentException("Shard '" + shard + "' is not migrated here. Migrated shards: " + primaries.keySet());
        }
        return Flyway.configure(getConfiguration().getClassLoader()).configuration(getConfiguration()).dataSource(primary).load();
    }

    /**
     * @return names of the shards this object migrates, in order
     */
    public java.util.List<String> shards() {
        return java.util.List.copyOf(primaries.keySet());
    }
}
