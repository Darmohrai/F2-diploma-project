package org.kurin.kurinspringbootstarter.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinNodeCustomizer;
import org.kurin.raft.KurinNode;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Metrics, tracing and Actuator support of the node. Every part is optional: it appears when its library is on
 * the classpath and its registry bean exists, and the node works the same without any of them.
 */
@AutoConfiguration(after = KurinAutoConfiguration.class, afterName = {
        // Spring Boot 3 names, then the names the same classes have in Spring Boot 4
        "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.observation.ObservationAutoConfiguration",
        "org.springframework.boot.micrometer.observation.autoconfigure.ObservationAutoConfiguration"})
@ConditionalOnBean(KurinNode.class)
public class KurinObservabilityAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class MetricsConfiguration {
        @Bean
        KurinRaftMetrics kurinRaftMetrics(MeterRegistry registry) {
            return new KurinRaftMetrics(registry);
        }

        @Bean
        KurinNodeCustomizer kurinRaftMetricsCustomizer(KurinRaftMetrics metrics) {
            return (node, services) -> {
                node.addObserver(metrics);
                metrics.bindTo(node);
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ObservationRegistry.class)
    @ConditionalOnBean(ObservationRegistry.class)
    static class TracingConfiguration {
        @Bean
        KurinNodeCustomizer kurinObservationCustomizer(ObservationRegistry registry) {
            return (node, services) -> node.instrumentation(new ObservationInstrumentation(registry));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.actuate.endpoint.annotation.Endpoint")
    static class EndpointConfiguration {
        @Bean
        @ConditionalOnMissingBean
        KurinEndpoint kurinEndpoint(KurinNode node, ObjectProvider<KurinEndpointContributor> contributors) {
            return new KurinEndpoint(node, contributors);
        }
    }
}
