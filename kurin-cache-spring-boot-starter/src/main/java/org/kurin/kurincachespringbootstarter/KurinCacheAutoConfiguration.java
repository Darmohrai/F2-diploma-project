package org.kurin.kurincachespringbootstarter;

import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinNodeBuilderCustomizer;
import org.kurin.kurintieredcache.core.*;
import org.kurin.kurintieredcache.provider.CaffeineCacheProvider;
import org.kurin.kurintieredcache.provider.RocksDBCacheProvider;
import org.kurin.network.serializer.KryoSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.util.List;

@AutoConfiguration
@ConditionalOnClass(KurinCacheManager.class)
@EnableAspectJAutoProxy
@AutoConfigureAfter(KurinAutoConfiguration.class)
public class KurinCacheAutoConfiguration {

    @Bean(destroyMethod = "close")
    public KurinCacheManager kurinCacheManager(ObjectProvider<KryoSerializer> cacheSerializerProvider) {

        KryoSerializer cacheSerializer = cacheSerializerProvider.getObject();

        List<CacheProvider> providers = List.of(
                new CaffeineCacheProvider(),
                new RocksDBCacheProvider()
        );
        CacheConfigurationContext context = new CacheConfigurationContext(cacheSerializer, "./kurin-data/cache");
        return new DefaultKurinCacheManager(providers, context);
    }

    @Bean
    public KurinCacheAspect kurinCacheAspect(KurinCacheManager cacheManager) {
        return new KurinCacheAspect(cacheManager);
    }

    @Bean
    public KurinNodeBuilderCustomizer raftCacheIntegrator(KurinCacheManager cacheManager) {
        return (builder, registeredServices) -> {
            RaftCacheUpdater updater = new RaftCacheUpdater(cacheManager, registeredServices);
            builder.addStateMachineListener(updater);
        };
    }
}
