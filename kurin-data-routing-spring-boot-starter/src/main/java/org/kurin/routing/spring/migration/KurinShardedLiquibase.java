package org.kurin.routing.spring.migration;

import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Runs the Liquibase migrations of the application on the primary of every shard, one shard after another.
 * <p>
 * It stands in for a {@link SpringLiquibase} that was configured for the routing data source, and uses that
 * very object to do the work: for each shard it is pointed at the primary of the shard and run. Nothing of its
 * configuration is copied or interpreted here, so every setting the application made (change log, contexts,
 * labels, schemas, parameters, anything a later version of Liquibase adds) applies to every shard as it is.
 * Replicas are not touched: they get the schema by replication.
 */
public class KurinShardedLiquibase extends SpringLiquibase {
    private static final Logger log = LoggerFactory.getLogger(KurinShardedLiquibase.class);

    private final SpringLiquibase configured;
    private final Map<String, DataSource> primaries;

    /**
     * @param configured the fully configured Liquibase of the application, not yet run
     * @param primaries  the primaries to migrate by shard name, in order
     */
    public KurinShardedLiquibase(SpringLiquibase configured, Map<String, DataSource> primaries) {
        this.configured = configured;
        this.primaries = primaries;
        // So that whoever asks this bean for its settings gets those of the application
        BeanUtils.copyProperties(configured, this);
    }

    @Override
    public void afterPropertiesSet() throws LiquibaseException {
        DataSource routing = configured.getDataSource();
        try {
            for (Map.Entry<String, DataSource> shard : primaries.entrySet()) {
                log.info("Running Liquibase migrations on the primary of shard '{}'", shard.getKey());
                configured.setDataSource(shard.getValue());
                try {
                    configured.afterPropertiesSet();
                } catch (LiquibaseException | RuntimeException e) {
                    throw new LiquibaseException("Liquibase migration of shard '" + shard.getKey() + "' failed: " + e.getMessage(), e);
                }
            }
        } finally {
            configured.setDataSource(routing);
        }
    }

    /**
     * @return names of the shards this object migrates, in order
     */
    public java.util.List<String> shards() {
        return java.util.List.copyOf(primaries.keySet());
    }
}
