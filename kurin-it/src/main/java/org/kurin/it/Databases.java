package org.kurin.it;

import org.kurin.kurintieredcache.api.ExternalCacheStore;
import org.kurin.routing.KurinDataSourceTopology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

/**
 * Two shards, each a primary and a replica. Every node is an H2 database of its own, named after the run, so
 * the instances of one test share them and different tests do not. The databases are not replicated: a row
 * written to a primary never shows up on its replica, which lets a test see where a read was served from.
 */
@Configuration(proxyBeanMethods = false)
public class Databases {

    public static DataSource node(String databaseId, String name) {
        return new DriverManagerDataSource("jdbc:h2:mem:" + databaseId + "-" + name + ";DB_CLOSE_DELAY=-1");
    }

    @Bean
    KurinDataSourceTopology topology(@Value("${it.database-id}") String databaseId) {
        return KurinDataSourceTopology.builder()
                .shard("eu", node(databaseId, "eu-primary"), node(databaseId, "eu-replica"))
                .shard("us", node(databaseId, "us-primary"), node(databaseId, "us-replica"))
                .build();
    }

    @Bean
    ExternalCacheStore externalCacheStore() {
        return Shared.EXTERNAL_CACHE;
    }
}
