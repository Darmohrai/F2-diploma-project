# Spring Boot compatibility of Kurin

Kurin 1.1.1 runs on Spring Boot 3.2 and later 3.x and on Spring Boot 4.x. This document records how that was
established: which classes of Spring Boot and Spring Framework each module uses, whether they differ between the
two generations, and what was done where they do.

Checked versions: Spring Boot **3.2.12** (Spring Framework 6.1) and Spring Boot **4.1.1** (Spring Framework
7.0.9), the latest stable 4.x on Maven Central when the work was done. The presence and location of every class
below was read from the jars of these versions (`jar tf`, `javap`), not from memory.

## How compatibility is kept

1. **The starters use only API that is the same in both generations.** They are compiled against Spring Boot 3.2
   and the resulting jars are used unchanged on Spring Boot 4.
2. **Classes of Spring Boot that moved are referred to by name**, with both names, in `beforeName` / `afterName`
   of `@AutoConfiguration` and in `@ConditionalOnClass(name = ...)`. A name that does not exist is ignored.
3. **API that differs lives in two adapter modules**: `kurin-actuator-boot3` and `kurin-actuator-boot4`. Only the
   health API needed this. What a health indicator reports is decided by classes without Spring
   (`KurinNodeHealth`, `KurinCacheHealth`, `KurinRoutingHealth`, `KurinRoutingDiagnostics`); an adapter is a
   translation of a few lines into `Health` of its generation.
4. **Dependencies of the starters exist under the same coordinates in both generations.** `spring-boot-starter-aop`
   was renamed to `spring-boot-starter-aspectj` in Spring Boot 4, so the starters depend on `spring-aop` and
   `aspectjweaver` directly.

## Verification

| What | How |
|---|---|
| Source compatibility of the starters with Spring Boot 4 | `./mvnw -Pboot4 clean install` compiles the starters against 4.1.1 and runs their tests on it |
| Binary compatibility: jars built on 3.2 running on 4 | default build: `kurin-actuator-boot4` and `kurin-it-boot4` import the 4.1.1 BOM and use the starter jars compiled against 3.2.12 |
| Binary compatibility the other way | `-Pboot4`: `kurin-actuator-boot3` and `kurin-it-boot3` keep 3.2.12 and use starter jars compiled against 4.1.1 |
| The same application on both | `kurin-it-boot3` and `kurin-it-boot4` compile and run the same sources (`kurin-it/src`) |

## Audit

"Same" means: same fully qualified name, and the members Kurin uses are present with the same signatures.

### Spring Boot classes

| Class (Spring Boot 3.2 name) | Used by | Spring Boot 4.1 | Handling |
|---|---|---|---|
| `o.s.b.autoconfigure.AutoConfiguration` | all starters, adapters | same (`spring-boot-autoconfigure`) | - |
| `o.s.b.autoconfigure.condition.ConditionalOnBean`, `ConditionalOnClass`, `ConditionalOnMissingBean`, `ConditionalOnProperty`, `ConditionalOnWebApplication` | all starters, adapters | same | - |
| `o.s.b.context.properties.ConfigurationProperties`, `EnableConfigurationProperties` | all starters | same (`spring-boot`) | - |
| `o.s.b.convert.DurationStyle` | locks, cache starters | same | - |
| `o.s.b.context.event.ApplicationReadyEvent` | routing starter | same | - |
| `o.s.b.actuate.endpoint.annotation.Endpoint`, `ReadOperation` | node starter (`/actuator/kurin`) | same (`spring-boot-actuator`) | stays in the starter, behind `@ConditionalOnClass(name = ...)` |
| `o.s.b.actuate.health.Health`, `HealthIndicator`, `HealthContributor`, `CompositeHealthContributor`, `Status` | health indicators | **moved** to `o.s.b.health.contributor` (new module `spring-boot-health`); composites are iterated as `HealthContributors.Entry` instead of `NamedContributor` | adapters |
| `o.s.b.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator` | health indicators | **moved** to `o.s.b.health.autoconfigure.contributor` | adapters |
| `o.s.b.actuate.jdbc.DataSourceHealthIndicator` | `db` health | **moved** to `o.s.b.jdbc.health` (new module `spring-boot-jdbc`) | adapters |
| `o.s.b.actuate.autoconfigure.jdbc.DataSourceHealthContributorAutoConfiguration` | `db` health, by name | **moved** to `o.s.b.jdbc.autoconfigure.health`; still yields to a bean named `dbHealthIndicator` or `dbHealthContributor` | each adapter names its own |
| `o.s.b.actuate.autoconfigure.metrics.MetricsAutoConfiguration`, `CompositeMeterRegistryAutoConfiguration`, `export.simple.SimpleMetricsExportAutoConfiguration` | all starters, by name (`afterName`) | **moved** to `o.s.b.micrometer.metrics.autoconfigure` (module `spring-boot-micrometer-metrics`) | both names listed |
| `o.s.b.actuate.autoconfigure.observation.ObservationAutoConfiguration` | node starter, routing starter, by name | **moved** to `o.s.b.micrometer.observation.autoconfigure` | both names listed |
| `o.s.b.autoconfigure.jdbc.DataSourceAutoConfiguration` | routing starter, by name (`beforeName`) | **moved** to `o.s.b.jdbc.autoconfigure` | both names listed |
| `o.s.b.autoconfigure.liquibase.LiquibaseAutoConfiguration` | routing starter (migrations per shard) | **moved** to `o.s.b.liquibase.autoconfigure` (module `spring-boot-liquibase`); in both it declares the bean `liquibase` of type `SpringLiquibase` | not referenced at all: a bean post-processor recognises the bean by its type `SpringLiquibase`, which belongs to Liquibase |
| `o.s.b.autoconfigure.flyway.FlywayAutoConfiguration`, `FlywayMigrationInitializer` | routing starter (migrations per shard) | **moved** to `o.s.b.flyway.autoconfigure` (module `spring-boot-flyway`); in both the initializer calls `migrate()` on the bean of type `Flyway` | not referenced: the `Flyway` bean is recognised by its type and replaced |
| `o.s.b.autoconfigure.orm.jpa.HibernatePropertiesCustomizer` | routing starter (mode `FAIL`) | **moved** to `o.s.b.hibernate.autoconfigure`; the single method `customize(Map<String, Object>)` is unchanged | implemented as a `java.lang.reflect.Proxy` of whichever interface is present |
| `o.s.b.autoconfigure.aop.AopAutoConfiguration` | tests | same | - |
| `o.s.b.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration`, `JdbcTemplateAutoConfiguration`, `o.s.b.autoconfigure.transaction.TransactionAutoConfiguration` | tests | **moved** to `o.s.b.jdbc.autoconfigure`, `o.s.b.transaction.autoconfigure` | tests look the class up under both names |
| `o.s.b.test.context.runner.ApplicationContextRunner`, `o.s.b.autoconfigure.AutoConfigurations`, `SpringApplicationBuilder`, `WebApplicationType`, `EnableAutoConfiguration`, `SpringBootTest` | tests | same | - |

### Dependencies

| Spring Boot 3.2 | Spring Boot 4.1 | Handling |
|---|---|---|
| `spring-boot-starter-aop` | renamed `spring-boot-starter-aspectj` | starters depend on `spring-aop` + `aspectjweaver` |
| `spring-boot-starter-web` | `spring-boot-starter-webmvc` (the old name remains as a deprecated alias) | only in the integration test modules, each uses its own |
| `spring-boot-actuator-autoconfigure` holds the health conditions | health is in `spring-boot-health` | adapter poms |
| one `spring-boot-autoconfigure` with all autoconfigurations | one module per technology (`spring-boot-jdbc`, `spring-boot-liquibase`, `spring-boot-flyway`, `spring-boot-hibernate`, ...) | nothing in the starters depends on these modules |
| `liquibase-core` 4.24, Flyway 9.22, Hibernate 6.4 | Liquibase 5.0, Flyway 12.4, Hibernate 7.4 | only API present in both is called: `SpringLiquibase.get/setDataSource`, `afterPropertiesSet`; `Flyway(Configuration)`, `getConfiguration`, `migrate`, `Flyway.configure(ClassLoader).configuration(...).dataSource(...).load()`. Hibernate 6.4 knows only `hibernate.temp.use_jdbc_metadata_defaults`, 7.4 also `hibernate.boot.allow_jdbc_metadata_access`: both are set |
| Jakarta Servlet 6.0 | 6.1 | filters and response wrappers use only 6.0 API |
| JUnit Jupiter 5.10 | 6.0 | Surefire 3.5.5 |
| Micrometer 1.12 | 1.17 | meters, `Observation`, `SenderContext`, `ReceiverContext` are unchanged |

### Spring Framework classes

All present with the same names and the used signatures in 6.1 and 7.0.9:

| Area | Classes | Used by |
|---|---|---|
| Beans, context | `BeanPostProcessor`, `BeanFactoryAware`, `ObjectProvider`, `InitializingBean`, `SmartLifecycle`, `ApplicationEventPublisher`, `ApplicationEvent`, `SmartApplicationListener`, `@Bean`, `@Configuration`, `@Primary`, `@EnableAspectJAutoProxy` | all starters |
| AOP | `AopUtils`, `AopProxyUtils`; AspectJ `@Aspect`, `@Around`, `ProceedingJoinPoint`, `MethodSignature` | all starters |
| Core | `AnnotatedElementUtils`, `Ordered`, `@Order`, `DefaultParameterNameDiscoverer`, `ReflectionUtils`, `ClassUtils`, `DataSize` | all starters |
| Environment | `EnvironmentAware`, `Environment.resolveRequiredPlaceholders` (placeholders in the duration attributes of the coordination annotations, since 1.1.2) | locks starter |
| SpEL | `SpelExpressionParser`, `Expression`, `ParseException`, `MethodBasedEvaluationContext` | locks, cache, routing starters |
| Transactions | `TransactionSynchronizationManager`, `TransactionSynchronization` | cache, routing starters |
| JDBC | `AbstractDataSource`, `DelegatingDataSource`, `LazyConnectionDataSourceProxy`, `JdbcTemplate`, `DataSourceTransactionManager` | routing starter |
| Web | `OncePerRequestFilter`, `ContentCachingResponseWrapper`, `RequestContextHolder`, `ServletRequestAttributes` | cache, routing starters (optional) |

The plain modules (`core`, `kurin-locks`, `kurin-tiered-cache`, `kurin-data-routing`, `kurin-test`) use no class of
Spring at all.

## Which adapter to add

| Application runs on | Add |
|---|---|
| Spring Boot 3.2 - 3.5 | `kurin-actuator-boot3` |
| Spring Boot 4.x | `kurin-actuator-boot4` |
| no Actuator | neither; metrics and `/actuator/kurin` do not need them, only the health indicators do |

An adapter of the other generation does no harm and no good: its autoconfiguration is conditional on the health
API it was built for, does not find it and stays inactive, so the health indicators of Kurin are simply absent.
