package org.kurin.kurinlocksspringbootstarter;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.command.AcquireLockCommand;
import org.kurin.kurinlocks.command.ReleaseLockCommand;
import org.kurin.kurinlocks.state.DistributedLockService;
import org.kurin.kurinlocksspringbootstarter.aop.KurinLocksAspect;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinNodeBuilderCustomizer;
import org.kurin.raft.KurinNode;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@AutoConfiguration
@ConditionalOnClass(KurinNode.class)
@EnableAspectJAutoProxy
@AutoConfigureAfter(KurinAutoConfiguration.class)
public class KurinLocksAutoConfiguration {

    @Bean
    public DistributedLockService distributedLockService() {
        return new DistributedLockService();
    }

    @Bean
    public KurinNodeBuilderCustomizer locksRaftIntegrator(DistributedLockService lockService) {
        return (builder, registeredServices) -> {
            builder.registerCommand(AcquireLockCommand.class);
            builder.registerCommand(ReleaseLockCommand.class);

            builder.addService(lockService);
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinLockManager kurinLockManager(KurinNode kurinNode) {
        return new KurinLockManager(kurinNode);
    }

    @Bean
    public KurinLocksAspect kurinLocksAspect(KurinLockManager lockManager, KurinNode kurinNode) {
        return new KurinLocksAspect(lockManager, kurinNode);
    }
}
