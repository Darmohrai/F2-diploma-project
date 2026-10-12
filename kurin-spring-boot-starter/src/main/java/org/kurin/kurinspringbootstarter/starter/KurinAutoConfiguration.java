package org.kurin.kurinspringbootstarter.starter;

import org.kurin.kurinspringbootstarter.template.KurinTemplate;
import org.kurin.network.config.NetworkConfig;
import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties({KurinProperties.class, KurinSerializationProperties.class})
public class KurinAutoConfiguration {

    // Static: a bean post-processor must exist before the configuration class itself is instantiated
    @Bean
    public static KurinServiceRegistry kurinServiceRegistry() {
        return new KurinServiceRegistry();
    }

    @Bean
    public KurinEventDispatcher kurinEventDispatcher(ApplicationEventPublisher eventPublisher) {
        return new KurinEventDispatcher(eventPublisher);
    }

    /**
     * The node is created here but started by {@link KurinNodeLifecycle}. Creating it needs no other
     * application bean, so anything may depend on it.
     */
    @Bean
    public KurinNode kurinNode(KurinProperties props, KurinEventDispatcher eventDispatcher,
                               ObjectProvider<KurinNodeBuilderCustomizer> customizers) {
        KurinNode.Builder builder = KurinNode.builder()
                .localNode(props.getAdvertisedHost(), props.getPort())
                .bindHost(props.getBindHost())
                .clusterName(props.getClusterName())
                .storage(props.getStorage())
                .addRoleChangeListener(eventDispatcher);
        if (props.getDataDir() != null) {
            builder.dataDir(props.getDataDir());
        }
        if (props.getPeers() != null) {
            builder.addPeers(props.getPeers().toArray(new String[0]));
        }

        RaftConfig raft = builder.raftConfig();
        raft.setHeartbeatIntervalMs(props.getHeartbeatInterval().toMillis());
        raft.setElectionTimeoutMinMs(props.getElectionTimeoutMin().toMillis());
        raft.setElectionTimeoutMaxMs(props.getElectionTimeoutMax().toMillis());
        raft.setLeaseRatio(props.getLeaseRatio());
        raft.setCommandTimeoutMs(props.getCommandTimeout().toMillis());
        raft.setMaxEntriesPerAppend(props.getMaxEntriesPerAppend());
        raft.setMaxInflightAppends(props.getMaxInflightAppends());
        raft.setSnapshotThresholdEntries(props.getSnapshotThreshold());
        raft.setDedupCacheSize(props.getDedupCacheSize());

        NetworkConfig network = builder.networkConfig();
        network.setMaxPayloadSize(Math.toIntExact(props.getMaxPayloadSize().toBytes()));
        network.setClusterSecret(props.getClusterSecret());
        network.setIdleTimeoutSeconds(Math.toIntExact(Math.max(1, props.getIdleTimeout().toSeconds())));
        NetworkConfig.TlsConfig tls = network.getTls();
        tls.setEnabled(props.getTls().isEnabled());
        tls.setKeyStore(props.getTls().getKeyStore());
        tls.setKeyStorePassword(props.getTls().getKeyStorePassword());
        tls.setKeyStoreType(props.getTls().getKeyStoreType());
        tls.setTrustStore(props.getTls().getTrustStore());
        tls.setTrustStorePassword(props.getTls().getTrustStorePassword());
        tls.setTrustStoreType(props.getTls().getTrustStoreType());

        customizers.orderedStream().forEach(customizer -> customizer.customize(builder));
        return builder.build();
    }

    @Bean
    public KurinNodeLifecycle kurinNodeLifecycle(KurinNode kurinNode, KurinServiceRegistry registry,
                                                 KurinSerializationProperties serialization,
                                                 ObjectProvider<KurinTypeRegistrar> typeRegistrars,
                                                 ObjectProvider<KurinNodeCustomizer> customizers) {
        return new KurinNodeLifecycle(kurinNode, registry, serialization, typeRegistrars, customizers);
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinTemplate kurinTemplate(KurinNode kurinNode) {
        return new KurinTemplate(kurinNode);
    }
}
