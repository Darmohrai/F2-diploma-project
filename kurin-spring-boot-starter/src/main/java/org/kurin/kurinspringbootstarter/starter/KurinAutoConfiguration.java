package org.kurin.kurinspringbootstarter.starter;

import org.kurin.kurinspringbootstarter.events.KurinLeaderElectedEvent;
import org.kurin.kurinspringbootstarter.events.KurinLeaderLostEvent;
import org.kurin.kurinspringbootstarter.template.KurinTemplate;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftStatus;
import org.kurin.raft.state.RoleChangeListener;
import org.kurin.raft.tools.PojoStateMachine;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;

@AutoConfiguration
@EnableConfigurationProperties(KurinProperties.class)
public class KurinAutoConfiguration {

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public KurinNode kurinNode(
            KurinProperties props,
            ApplicationContext context,
            ObjectProvider<KurinNodeBuilderCustomizer> customizers,
            ApplicationEventPublisher eventPublisher) {

        KurinNode.Builder builder = KurinNode.builder()
                .localNode(props.getHost(), props.getPort())
                .storage(props.getStorage());
        if (props.getDataDir() != null) {
            builder.dataDir(props.getDataDir());
        }
        if (props.getPeers() != null && !props.getPeers().isEmpty()) {
            builder.addPeers(props.getPeers().toArray(new String[0]));
        }

        builder.addRoleChangeListener(new RoleChangeListener() {
            @Override
            public void onLeaderElected(RaftStatus status) {
                eventPublisher.publishEvent(new KurinLeaderElectedEvent(this));
            }

            @Override
            public void onLeaderLost(RaftStatus status) {
                eventPublisher.publishEvent(new KurinLeaderLostEvent(this));
            }
        });

        List<Object> servicesList = new ArrayList<>();
        for (String beanName : context.getBeanDefinitionNames()) {
            Class<?> type = context.getType(beanName);
            if (type == null || type.getName().startsWith("org.springframework") || !PojoStateMachine.isService(type)) {
                continue;
            }
            Object bean = context.getBean(beanName);
            builder.addService(bean, AopUtils.getTargetClass(bean));
            servicesList.add(bean);
        }

        customizers.orderedStream().forEach(customizer -> customizer.customize(builder, servicesList));
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public KryoSerializer kurinSerializer(KurinNode kurinNode) {
        return kurinNode.serializer();
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinTemplate kurinTemplate(KurinNode kurinNode) {
        return new KurinTemplate(kurinNode);
    }
}
