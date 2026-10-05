package org.kurin.kurinspringbootstarter.starter;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinRestore;
import org.kurin.api.KurinSnapshot;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.KurinNode;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@AutoConfiguration
@EnableConfigurationProperties(KurinProperties.class)
public class KurinAutoConfiguration {

    @Bean
    public KryoSerializer kryoSerializer(ApplicationContext context) {
        List<Class<?>> registeredClasses = new ArrayList<>();
        for (String beanName : context.getBeanDefinitionNames()) {
            Class<?> type = context.getType(beanName);
            if (type != null && !type.getName().startsWith("org.springframework")) {
                try {
                    Object bean = context.getBean(beanName);
                    Class<?> targetClass = AopUtils.getTargetClass(bean);
                    for (Method method : targetClass.getDeclaredMethods()) {
                        if (method.isAnnotationPresent(KurinCommand.class)) {
                            registeredClasses.add(method.getParameterTypes()[0]);
                        }
                    }
                } catch (Exception ignored) {}
            }
        }
        return new KryoSerializer(registeredClasses);
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public KurinNode kurinNode(
            KurinProperties props,
            ApplicationContext context,
            ObjectProvider<KurinNodeBuilderCustomizer> customizers) {

        KurinNode.Builder builder = KurinNode.builder()
                .localNode(props.getHost(), props.getPort());

        if (props.getPeers() != null && !props.getPeers().isEmpty()) {
            builder.addPeers(props.getPeers().toArray(new String[0]));
        }

        Set<Object> registeredServices = new HashSet<>();
        List<Object> servicesList = new ArrayList<>();

        for (String beanName : context.getBeanDefinitionNames()) {
            Class<?> type = context.getType(beanName);
            if (type == null || type.getName().startsWith("org.springframework")) {
                continue;
            }
            try {
                Object bean = context.getBean(beanName);
                Class<?> targetClass = AopUtils.getTargetClass(bean);
                boolean isKurinService = false;

                for (Method method : targetClass.getDeclaredMethods()) {
                    if (method.isAnnotationPresent(KurinCommand.class)) {
                        builder.registerCommand(method.getParameterTypes()[0]);
                        isKurinService = true;
                    } else if (method.isAnnotationPresent(KurinSnapshot.class) ||
                            method.isAnnotationPresent(KurinRestore.class)) {
                        isKurinService = true;
                    }
                }

                if (isKurinService) {
                    if (registeredServices.add(bean)) {
                        builder.addService(bean);
                        servicesList.add(bean);
                    }
                }
            } catch (Exception ignored) {}
        }

        customizers.orderedStream().forEach(customizer -> customizer.customize(builder, servicesList));
        return builder.build();
    }
}
