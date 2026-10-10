package org.kurin.kurinlocksspringbootstarter;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.state.DistributedLockService;
import org.kurin.kurinlocksspringbootstarter.aop.KurinLocksAspect;
import org.kurin.kurinlocksspringbootstarter.template.KurinLockTemplate;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.raft.KurinNode;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@AutoConfiguration
@ConditionalOnClass(KurinNode.class)
@EnableAspectJAutoProxy
@AutoConfigureAfter(KurinAutoConfiguration.class)
@EnableConfigurationProperties(KurinLocksProperties.class)
public class KurinLocksAutoConfiguration {

    @Bean
    public DistributedLockService distributedLockService() {
        return new DistributedLockService();
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinLockManager kurinLockManager(KurinNode kurinNode, KurinLocksProperties props) {
        return new KurinLockManager(kurinNode, props.getLockTimeoutMs(), props.getUnlockTimeoutMs());
    }

    @Bean
    public KurinLocksAspect kurinLocksAspect(KurinLockManager lockManager, KurinNode kurinNode) {
        return new KurinLocksAspect(lockManager, kurinNode);
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinLockTemplate kurinLockTemplate(KurinLockManager lockManager) {
        return new KurinLockTemplate(lockManager);
    }
}
