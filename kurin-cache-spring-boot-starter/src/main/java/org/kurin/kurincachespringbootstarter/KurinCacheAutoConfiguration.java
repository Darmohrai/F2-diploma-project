package org.kurin.kurincachespringbootstarter;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinNodeCustomizer;
import org.kurin.kurintieredcache.core.*;
import org.kurin.kurintieredcache.provider.CaffeineCacheProvider;
import org.kurin.kurintieredcache.provider.RocksDBCacheProvider;
import org.kurin.network.serializer.KryoSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.util.List;

@AutoConfiguration
@ConditionalOnClass(KurinCacheManager.class)
@EnableAspectJAutoProxy
@AutoConfigureAfter(KurinAutoConfiguration.class)
@EnableConfigurationProperties(KurinCacheProperties.class)
public class KurinCacheAutoConfiguration {

    @Bean(destroyMethod = "close")
    public KurinCacheManager kurinCacheManager(
            ObjectProvider<KryoSerializer> cacheSerializerProvider,
            ObjectProvider<CacheProvider> externalProviders,
            KurinCacheProperties props) {

        // Independent of the node serializer: the cache manager is created while the node is still being built
        KryoSerializer cacheSerializer = KryoSerializer.standalone();

        List<CacheProvider> providers = new java.util.ArrayList<>(List.of(
                new CaffeineCacheProvider(),
                new RocksDBCacheProvider()
        ));
        externalProviders.orderedStream().forEach(providers::add);

        CacheConfigurationContext context = new CacheConfigurationContext(
                cacheSerializer,
                props.getStorageDir(),
                props.getL1MaxSize(),
                props.getL1ExpireMinutes(),
                props.getL3MaxFailures(),
                props.getL3CircuitOpenTimeoutMs()
        );

        return new DefaultKurinCacheManager(providers, context);
    }

    @Bean
    public KurinCacheAspect kurinCacheAspect(KurinCacheManager cacheManager, ObjectProvider<KurinLockManager> lockManagerProvider) {
        return new KurinCacheAspect(cacheManager, lockManagerProvider);
    }

    @Bean
    public KurinNodeCustomizer raftCacheIntegrator(KurinCacheManager cacheManager) {
        return (node, registeredServices) -> node.addStateMachineListener(new RaftCacheUpdater(cacheManager, registeredServices));
    }
}
