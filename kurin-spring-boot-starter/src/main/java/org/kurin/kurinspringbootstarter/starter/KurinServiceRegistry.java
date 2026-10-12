package org.kurin.kurinspringbootstarter.starter;

import org.kurin.raft.tools.PojoStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Collects the beans that declare {@code @KurinCommand}, {@code @KurinQuery}, {@code @KurinSnapshot} or
 * {@code @KurinRestore} methods, as the container creates them.
 * <p>
 * Being a bean post-processor, it never asks the container for a bean. A service can therefore inject
 * {@code KurinNode} or {@code KurinTemplate} without creating a dependency cycle.
 */
public class KurinServiceRegistry implements BeanPostProcessor, BeanFactoryAware {
    private static final Logger log = LoggerFactory.getLogger(KurinServiceRegistry.class);

    /**
     * @param target the service object itself, never a proxy
     * @param type   the class declaring the annotated methods
     */
    public record RegisteredService(String beanName, Object target, Class<?> type) {
    }

    private final List<RegisteredService> services = new ArrayList<>();
    private final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    private BeanFactory beanFactory;
    private boolean frozen;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public synchronized Object postProcessAfterInitialization(Object bean, String beanName) {
        Class<?> type = AopUtils.getTargetClass(bean);
        if (type.getName().startsWith("org.springframework.") || !declaresHandlers(type) || !isSingleton(beanName)) {
            return bean;
        }
        // Handlers run on the Raft thread against the service itself, not through its proxies
        Object target = AopProxyUtils.getSingletonTarget(bean);
        if (target == null) {
            target = bean;
        }
        if (!seen.add(target)) {
            return bean;
        }
        if (frozen) {
            throw new IllegalStateException("Bean '" + beanName + "' (" + type.getName() + ") declares Kurin handler methods "
                    + "but was created after the Kurin node started. State machine services must be eagerly initialized "
                    + "singletons: remove @Lazy from it and do not enable global lazy initialization for it.");
        }
        services.add(new RegisteredService(beanName, target, type));
        log.debug("Registered Kurin service bean '{}' ({})", beanName, type.getName());
        return bean;
    }

    private static boolean declaresHandlers(Class<?> type) {
        try {
            return PojoStateMachine.isService(type);
        } catch (LinkageError e) {
            // A bean class that references optional, absent dependencies cannot be reflected on. It is not ours.
            log.debug("Cannot inspect {} for Kurin handlers: {}", type.getName(), e.toString());
            return false;
        }
    }

    private boolean isSingleton(String beanName) {
        try {
            return beanFactory == null || !beanFactory.containsBean(beanName) || beanFactory.isSingleton(beanName);
        } catch (NoSuchBeanDefinitionException e) {
            return true; // registered programmatically as a singleton instance
        }
    }

    /**
     * Closes the registry: the node is about to be built from its content.
     */
    public synchronized void freeze() {
        frozen = true;
    }

    public synchronized List<RegisteredService> services() {
        return List.copyOf(services);
    }
}
