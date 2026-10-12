package org.kurin.kurincachespringbootstarter;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinNodeCustomizer;
import org.kurin.kurintieredcache.api.CacheClusterLock;
import org.kurin.kurintieredcache.api.ExternalCacheStore;
import org.kurin.kurintieredcache.api.KeyGenerator;
import org.kurin.kurintieredcache.api.KurinCacheInvalidator;
import org.kurin.kurintieredcache.api.KurinCacheObserver;
import org.kurin.kurintieredcache.api.KurinCacheValueCodec;
import org.kurin.kurintieredcache.core.CacheReader;
import org.kurin.kurintieredcache.core.CacheSettings;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.kurintieredcache.invalidation.CacheInvalidationStateMachine;
import org.kurin.kurintieredcache.invalidation.DefaultCacheInvalidator;
import org.kurin.kurintieredcache.key.DefaultKeyGenerator;
import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftConfig;
import org.kurin.raft.RaftNode;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.nio.file.Path;

@AutoConfiguration(after = KurinAutoConfiguration.class)
@ConditionalOnClass(KurinCacheManager.class)
@EnableAspectJAutoProxy
@EnableConfigurationProperties(KurinCacheProperties.class)
public class KurinCacheAutoConfiguration {

    // Static: a bean post-processor must exist before the configuration class itself is instantiated
    @Bean
    public static KurinCacheAnnotationValidator kurinCacheAnnotationValidator() {
        return new KurinCacheAnnotationValidator();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public KurinCacheManager kurinCacheManager(KurinCacheProperties props, KurinNode kurinNode,
                                               ObjectProvider<ExternalCacheStore> externalStore,
                                               ObjectProvider<KurinCacheValueCodec> codec,
                                               ObjectProvider<KurinCacheObserver> observers) {
        Path dataDirectory = props.getDataDir() != null ? props.getDataDir()
                : RaftNode.resolveDataDir(kurinNode.config(), kurinNode.getLocalAddress()).resolve("cache");

        CacheSettings defaults = props.defaultSettings();
        KurinCacheManager.Builder builder = KurinCacheManager.builder()
                .defaults(defaults)
                .dataDirectory(dataDirectory)
                .allowedPackages(props.getCodec().getAllowedPackages())
                .externalStore(externalStore.getIfAvailable())
                .externalTimeout(props.getL3().getTimeout())
                .externalCircuit(props.getL3().getFailureThreshold(), props.getL3().getOpenDuration())
                .externalWriteStripes(props.getL3().getWriteStripes())
                .sweep(props.getL2().getSweepInterval(), props.getL2().getSweepBatchSize())
                .epochCacheSize(props.getInvalidation().getEpochCacheSize())
                // With an in-memory Raft log nothing replays the invalidations a stopped instance missed,
                // so what it left on disk cannot be trusted
                .clearDiskOnStart(kurinNode.config().getStorage() == RaftConfig.StorageType.MEMORY);
        codec.ifAvailable(builder::codec);
        props.getCaches().forEach((name, cache) -> builder.cache(name, cache.applyTo(defaults)));
        observers.orderedStream().forEach(builder::observer);
        return builder.build();
    }

    /**
     * A state machine service of the node, discovered like any application service.
     */
    @Bean
    public CacheInvalidationStateMachine kurinCacheInvalidationStateMachine(KurinCacheManager cacheManager) {
        return new CacheInvalidationStateMachine(cacheManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinCacheInvalidator kurinCacheInvalidator(KurinNode kurinNode, KurinCacheManager cacheManager,
                                                      CacheInvalidationStateMachine stateMachine, KurinCacheProperties props) {
        return new DefaultCacheInvalidator(kurinNode, cacheManager, stateMachine, props.getInvalidation().getTimeout());
    }

    @Bean
    @ConditionalOnMissingBean
    public KeyGenerator kurinCacheKeyGenerator() {
        return new DefaultKeyGenerator();
    }

    @Bean
    public CacheExpressions kurinCacheExpressions() {
        return new CacheExpressions();
    }

    @Bean
    public CacheAnnotationSupport kurinCacheAnnotationSupport(KurinCacheManager cacheManager, CacheExpressions expressions,
                                                              KeyGenerator keyGenerator) {
        return new CacheAnnotationSupport(cacheManager, expressions, keyGenerator);
    }

    /**
     * Cluster-wide stampede protection needs the lock manager of the locks starter. Without it the reader
     * falls back to per-instance protection.
     */
    @Bean(destroyMethod = "close")
    public CacheReader kurinCacheReader(KurinCacheManager cacheManager, ObjectProvider<CacheClusterLock> clusterLock,
                                        ObjectProvider<KurinLockManager> lockManager) {
        CacheClusterLock lock = clusterLock.getIfAvailable(() -> {
            KurinLockManager manager = lockManager.getIfAvailable();
            return manager != null ? new LockManagerClusterLock(manager) : null;
        });
        return new CacheReader(cacheManager, lock);
    }

    @Bean
    public KurinCacheAspect kurinCacheAspect(CacheAnnotationSupport support, CacheReader reader, KurinCacheInvalidator invalidator) {
        return new KurinCacheAspect(support, reader, invalidator);
    }

    @Bean
    public KurinNodeCustomizer kurinCommandCacheCustomizer(CacheAnnotationSupport support) {
        return (node, services) -> node.addStateMachineListener(new CommandCacheListener(support, services));
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnClass(name = {"org.springframework.web.filter.OncePerRequestFilter", "jakarta.servlet.Filter"})
    @ConditionalOnProperty(prefix = "kurin.cache.web.trace-header", name = "enabled", havingValue = "true")
    static class TraceHeaderConfiguration {
        @Bean
        KurinCacheTraceFilter kurinCacheTraceFilter() {
            return new KurinCacheTraceFilter();
        }
    }
}
