package org.kurin.kurinlocksspringbootstarter;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.job.JobStateMachine;
import org.kurin.kurinlocks.job.KurinJobs;
import org.kurin.kurinlocks.leader.KurinLeaderTasks;
import org.kurin.kurinlocks.state.LockStateMachine;
import org.kurin.kurinlocksspringbootstarter.aop.KurinLocksAspect;
import org.kurin.kurinlocksspringbootstarter.template.KurinLockTemplate;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.raft.KurinNode;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@AutoConfiguration(after = KurinAutoConfiguration.class)
@ConditionalOnClass(KurinNode.class)
@EnableAspectJAutoProxy
@EnableConfigurationProperties({KurinLocksProperties.class, KurinJobsProperties.class})
public class KurinLocksAutoConfiguration {

    // Static: a bean post-processor must exist before the configuration class itself is instantiated
    @Bean
    public static KurinCoordinationRegistry kurinCoordinationRegistry() {
        return new KurinCoordinationRegistry();
    }

    /**
     * The replicated lock table. A state machine service of the node, discovered like any application service.
     */
    @Bean
    public LockStateMachine kurinLockStateMachine() {
        return new LockStateMachine();
    }

    @Bean
    public JobStateMachine kurinJobStateMachine() {
        return new JobStateMachine();
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinLockManager kurinLockManager(KurinNode kurinNode, LockStateMachine lockStateMachine, KurinLocksProperties props) {
        return new KurinLockManager(kurinNode, lockStateMachine, props.getCommandTimeout());
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinLeaderTasks kurinLeaderTasks(KurinNode kurinNode) {
        return new KurinLeaderTasks(kurinNode);
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinJobs kurinJobs(KurinNode kurinNode, KurinJobsProperties props) {
        return new KurinJobs(kurinNode, props.getCommandTimeout(), props.getHistorySize());
    }

    @Bean
    public KurinCoordinationLifecycle kurinCoordinationLifecycle(KurinNode kurinNode, KurinLeaderTasks leaderTasks,
                                                                 KurinCoordinationRegistry registry, BeanFactory beanFactory,
                                                                 KurinLocksProperties props) {
        return new KurinCoordinationLifecycle(kurinNode, leaderTasks, registry, beanFactory, props);
    }

    @Bean
    public KurinLocksAspect kurinLocksAspect(KurinLockManager lockManager, KurinJobs jobs, KurinNode kurinNode) {
        return new KurinLocksAspect(lockManager, jobs, kurinNode);
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinLockTemplate kurinLockTemplate(KurinLockManager lockManager) {
        return new KurinLockTemplate(lockManager);
    }
}
