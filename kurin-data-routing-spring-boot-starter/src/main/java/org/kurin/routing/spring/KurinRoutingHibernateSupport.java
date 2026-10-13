package org.kurin.routing.spring;

import org.kurin.routing.KurinDataSourceTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;

/**
 * Keeps Hibernate from asking the database for its metadata when nothing may reach a database without a shard.
 * <p>
 * At startup Hibernate opens a connection to find out the dialect and the capabilities of the driver. With
 * several shards and {@code kurin.routing.unrouted.mode=fail} that connection is refused. So in that case, and
 * only if the application did not decide the matter itself, Hibernate is told not to look, and the dialect has
 * to be named in the configuration. In the default mode {@code startup-only} nothing is changed: Hibernate reads
 * the metadata from the primary of the unrouted shard.
 * <p>
 * The hook Spring Boot offers for this, {@code HibernatePropertiesCustomizer}, lives in different packages in
 * Spring Boot 3 and 4, so it is implemented as a proxy of whichever is present.
 */
public class KurinRoutingHibernateSupport implements BeanDefinitionRegistryPostProcessor, BeanFactoryAware, EnvironmentAware {
    private static final Logger log = LoggerFactory.getLogger(KurinRoutingHibernateSupport.class);

    private static final List<String> CUSTOMIZER_INTERFACES = List.of(
            "org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer",
            "org.springframework.boot.hibernate.autoconfigure.HibernatePropertiesCustomizer");
    // The setting of Hibernate 6.5 and later, and the one of the versions before
    static final String ALLOW_METADATA_ACCESS = "hibernate.boot.allow_jdbc_metadata_access";
    static final String USE_METADATA_DEFAULTS = "hibernate.temp.use_jdbc_metadata_defaults";

    private BeanFactory beanFactory;
    private Environment environment;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        ClassLoader classLoader = getClass().getClassLoader();
        for (String name : CUSTOMIZER_INTERFACES) {
            if (ClassUtils.isPresent(name, classLoader)) {
                registerCustomizer(registry, ClassUtils.resolveClassName(name, classLoader));
            }
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        // Only bean definitions are added, above
    }

    private <T> void registerCustomizer(BeanDefinitionRegistry registry, Class<T> customizerInterface) {
        InvocationHandler handler = (proxy, method, arguments) -> {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == arguments[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "KurinRoutingHibernateSupport";
                };
            }
            if (arguments != null && arguments.length == 1 && arguments[0] instanceof Map<?, ?> properties) {
                @SuppressWarnings("unchecked")
                Map<String, Object> hibernateProperties = (Map<String, Object>) properties;
                customize(hibernateProperties);
                return null;
            }
            throw new UnsupportedOperationException("Unexpected call of " + method);
        };
        GenericBeanDefinition definition = new GenericBeanDefinition();
        definition.setBeanClass(customizerInterface);
        definition.setInstanceSupplier(() -> customizerInterface.cast(
                Proxy.newProxyInstance(customizerInterface.getClassLoader(), new Class<?>[]{customizerInterface}, handler)));
        registry.registerBeanDefinition("kurinRoutingHibernatePropertiesCustomizer", definition);
    }

    /**
     * @throws IllegalStateException if Hibernate may not look at the database and is not told the dialect either
     */
    void customize(Map<String, Object> hibernateProperties) {
        KurinDataSourceTopology topology = beanFactory.getBeanProvider(KurinDataSourceTopology.class).getIfAvailable();
        KurinRoutingProperties props = beanFactory.getBeanProvider(KurinRoutingProperties.class).getIfAvailable();
        if (topology == null || props == null || !topology.isSharded()
                || props.getUnrouted().getMode() != KurinRoutingProperties.UnroutedMode.FAIL) {
            return;
        }
        if (hibernateProperties.containsKey(ALLOW_METADATA_ACCESS) || hibernateProperties.containsKey(USE_METADATA_DEFAULTS)) {
            // The application decided for itself
            return;
        }
        boolean dialectNamed = hibernateProperties.containsKey("hibernate.dialect")
                || environment.containsProperty("spring.jpa.database-platform")
                || environment.containsProperty("spring.jpa.database");
        if (!dialectNamed) {
            throw new IllegalStateException("kurin.routing.unrouted.mode=fail forbids database access without a shard, so "
                    + "Hibernate cannot ask the database which dialect to use. Name the dialect with "
                    + "spring.jpa.database-platform, or use kurin.routing.unrouted.mode=startup-only, which lets such "
                    + "access through to shard '" + unroutedShard(topology, props) + "' while the application starts.");
        }
        hibernateProperties.put(ALLOW_METADATA_ACCESS, false);
        hibernateProperties.put(USE_METADATA_DEFAULTS, false);
        log.info("Hibernate will not read JDBC metadata at startup: with several shards and kurin.routing.unrouted.mode=fail "
                + "no connection can be opened without a shard. The dialect comes from the configuration.");
    }

    private static String unroutedShard(KurinDataSourceTopology topology, KurinRoutingProperties props) {
        String shard = props.getUnrouted().getShard();
        return shard != null ? shard : topology.shards().keySet().iterator().next();
    }
}
