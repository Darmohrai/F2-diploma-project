package org.kurin.routing.spring;

import org.kurin.routing.KurinShardRouter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

@AutoConfiguration(beforeName = "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration")
@ConditionalOnClass(AbstractRoutingDataSource.class)
@ConditionalOnBean(KurinDataSourceTopology.class)
@EnableAspectJAutoProxy
@EnableConfigurationProperties(KurinRoutingProperties.class)
public class KurinRoutingAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = {
            "org.springframework.web.context.request.ServletRequestAttributes",
            "jakarta.servlet.http.HttpServletRequest"
    })
    static class ServletClientIdConfiguration {
        @Bean
        @ConditionalOnMissingBean(KurinClientIdResolver.class)
        public KurinClientIdResolver servletKurinClientIdResolver() {
            return new ServletKurinClientIdResolver();
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinClientIdResolver kurinClientIdResolver() {
        return () -> null;
    }

    @Bean
    @ConditionalOnMissingBean
    public SessionStickyManager sessionStickyManager(KurinRoutingProperties props) {
        return new SessionStickyManager(props.getStickyWindowMs());
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinShardRouter kurinShardRouter(KurinDataSourceTopology topology, KurinRoutingProperties props) {
        return new KurinShardRouter(topology.shards().keySet(), props.getVirtualNodes());
    }

    @Bean
    @Primary
    public KurinRoutingDataSource kurinRoutingDataSource(KurinDataSourceTopology topology,
                                                         SessionStickyManager stickyManager,
                                                         KurinClientIdResolver clientIdResolver) {
        return new KurinRoutingDataSource(topology, stickyManager, clientIdResolver);
    }

    @Bean
    public KurinRoutingAspect kurinRoutingAspect(SessionStickyManager stickyManager,
                                                 KurinClientIdResolver clientIdResolver,
                                                 KurinShardRouter shardRouter) {
        return new KurinRoutingAspect(stickyManager, clientIdResolver, shardRouter);
    }
}
