package org.kurin.routing.spring.migration;

import liquibase.integration.spring.SpringLiquibase;
import org.flywaydb.core.Flyway;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.spring.KurinRoutingAutoConfiguration;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Map;

/**
 * With several shards, makes the Liquibase or Flyway migrations of the application run on the primary of every
 * shard.
 * <p>
 * Spring Boot configures one migration run against the primary {@code DataSource}, which here is the routing
 * data source: without a shard it would reach one database at best. The migration bean Spring Boot (or the
 * application) created for the routing data source is therefore replaced, before it runs, by one that does the
 * same work on each shard. A migration bean with a data source of its own ({@code spring.liquibase.url},
 * {@code @LiquibaseDataSource}, {@code spring.flyway.url}) is left alone, and so is everything when the
 * topology has a single shard.
 * <p>
 * Switch off with {@code kurin.routing.migrations.enabled=false}; pick the shards with
 * {@code kurin.routing.migrations.shards}.
 */
@AutoConfiguration(after = KurinRoutingAutoConfiguration.class)
@ConditionalOnBean(KurinDataSourceTopology.class)
public class KurinShardedMigrationsAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "liquibase.integration.spring.SpringLiquibase")
    static class LiquibaseConfiguration {
        @Bean
        static BeanPostProcessor kurinShardedLiquibaseProcessor(BeanFactory beans) {
            ShardedMigrationTargets targets = new ShardedMigrationTargets(beans);
            return new BeanPostProcessor() {
                // Before initialization: initializing a SpringLiquibase is what runs the migration
                @Override
                public Object postProcessBeforeInitialization(Object bean, String beanName) {
                    if (bean instanceof SpringLiquibase liquibase && !(bean instanceof KurinShardedLiquibase)) {
                        Map<String, DataSource> primaries = targets.primariesFor(liquibase.getDataSource());
                        if (!primaries.isEmpty()) {
                            return new KurinShardedLiquibase(liquibase, primaries);
                        }
                    }
                    return bean;
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.flywaydb.core.Flyway")
    static class FlywayConfiguration {
        @Bean
        static BeanPostProcessor kurinShardedFlywayProcessor(BeanFactory beans) {
            ShardedMigrationTargets targets = new ShardedMigrationTargets(beans);
            return new BeanPostProcessor() {
                // The migration is started later, by whoever holds the Flyway bean; it gets this one
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (bean instanceof Flyway flyway && !(bean instanceof KurinShardedFlyway)) {
                        Map<String, DataSource> primaries = targets.primariesFor(flyway.getConfiguration().getDataSource());
                        if (!primaries.isEmpty()) {
                            return new KurinShardedFlyway(flyway.getConfiguration(), primaries);
                        }
                    }
                    return bean;
                }
            };
        }
    }
}
