# РОЗДІЛ 2. ПРОЄКТУВАННЯ АРХІТЕКТУРИ СИСТЕМИ

Розділ спирається на інженерний контракт [srs.md](./srs.md) (ISO/IEC/IEEE 29148) і фіксує архітектуру Kurin за
моделлю C4 (рівні 1–3), архітектурні рішення ([adr/](./adr/)), модель даних, контракти протоколу й алгоритми
критичних вузлів. Діаграми записано кодом Mermaid і відображаються GitHub без додаткових інструментів.

Позначення трасування: `→ SRS-FUN-NN` – вимога, яку реалізує елемент; `→ ADR-NNN` – рішення, на якому він
ґрунтується.

---

## 2.1. Контекстне та контейнерне моделювання системи (C4 Model)

<a id="diagram-level1"></a>

### 2.1.1. Рівень 1 – System Context

```mermaid
C4Context
    title Kurin – System Context (C4 Level 1)

    Person(dev, "Розробник застосунку", "Підключає starters, позначає методи анотаціями, пише обробники команд")
    Person(ops, "Оператор (DevOps)", "Задає склад кластера, сертифікати й таймінги, стежить за станом")
    Person_Ext(client, "Клієнт застосунку", "Користувач або сервіс, що викликає HTTP API застосунку")

    System(kurin, "Кластер сервісу з Kurin", "3–5 екземплярів Java-сервісу з вбудованим вузлом Raft: блокування, задачі лідера, одноразові задачі, кеш L1–L3, маршрутизація БД")

    System_Ext(gw, "Ingress / API Gateway", "TLS, перевірка JWT, rate limiting, балансування")
    System_Ext(pg, "PostgreSQL", "Primary і репліки з потоковою реплікацією")
    System_Ext(ext, "Зовнішнє сховище кешу", "Реалізація ExternalCacheStore, напр. Redis")
    System_Ext(mon, "Моніторинг", "Prometheus + Grafana")

    Rel(dev, kurin, "Підключає бібліотеку, анотації, властивості kurin.*", "Maven, Java API")
    Rel(client, gw, "Бізнес-запити", "HTTPS, JWT Bearer")
    Rel(gw, kurin, "Запити до екземплярів, маркер узгодженості", "HTTP/1.1, X-Kurin-Consistency")
    Rel(ops, gw, "Перевірка стану вузлів", "HTTPS, JWT Bearer, /actuator/*")
    Rel(kurin, pg, "SQL, позиція WAL", "JDBC / PostgreSQL protocol, TCP 5432, TLS")
    Rel(kurin, ext, "Читання й запис рівня L3", "RESP, TCP 6379, TLS")
    Rel(mon, kurin, "Збирання метрик", "HTTP GET /actuator/prometheus")

    UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="1")
```

**Опис елементів рівня 1**

| Елемент | Тип | Роль | Трасування |
| :--- | :--- | :--- | :--- |
| Розробник застосунку | Person | Додає залежність starter, описує команди як Java-record і позначає методи `@KurinCommand`, `@KurinDistributedLock`, `@KurinCacheable`, `@KurinReadOnly`. | [SRS-FUN-30](./srs.md#srs-fun-30), [SRS-FUN-31](./srs.md#srs-fun-31) |
| Оператор | Person | Задає `kurin.node.peers`, сертифікати mTLS і `clusterSecret`; читає `/actuator/health` і `/actuator/kurin`. | [SRS-FUN-33](./srs.md#srs-fun-33) |
| Клієнт застосунку | Person_Ext | Викликає бізнес-API; між запитами переносить маркер узгодженості в заголовку `X-Kurin-Consistency`. | [SRS-FUN-26](./srs.md#srs-fun-26) |
| Кластер сервісу з Kurin | System | Межа системи: усі екземпляри сервісу разом із вбудованими вузлами Raft і їхніми локальними сховищами. | [SRS](./srs.md#1-контекст-системи-system-context) |
| Ingress / API Gateway | System_Ext | Термінує TLS клієнтів, перевіряє JWT, обмежує частоту запитів. Kurin власного HTTP-порту не має. | [SRS-CON-01](./srs.md#srs-con-01), [SRS-CON-07](./srs.md#srs-con-07) |
| PostgreSQL | System_Ext | Дані застосунку; primary приймає запис, репліки – читання. | [SRS-FUN-24](./srs.md#srs-fun-24) … [SRS-FUN-29](./srs.md#srs-fun-29) |
| Зовнішнє сховище кешу | System_Ext | Рівень L3, спільний для екземплярів; підключається реалізацією `ExternalCacheStore`. | [SRS-FUN-19](./srs.md#srs-fun-19) |
| Моніторинг | System_Ext | Забирає метрики Micrometer у форматі Prometheus. | [SRS-FUN-32](./srs.md#srs-fun-32) |

<a id="diagram-level2"></a>

### 2.1.2. Рівень 2 – Container Diagram

Контейнер у C4 – окремо запущена одиниця виконання або сховище даних. Модулі Kurin (`core`, `kurin-locks`,
`kurin-tiered-cache`, `kurin-data-routing`, starters) **не є контейнерами**: вони збираються в один образ
застосунку і працюють у його JVM. Тому кластер складається з N однакових контейнерів Application Instance,
кожен із двома локальними томами.

```mermaid
flowchart TB
    client["<b>Клієнт застосунку</b><br/>[Person]"]
    ops["<b>Оператор (DevOps)</b><br/>[Person]"]
    gw["<b>Ingress / API Gateway</b><br/>[Container: NGINX / Spring Cloud Gateway]<br/>TLS, JWT, rate limiting"]
    prom["<b>Prometheus</b><br/>[Container]<br/>Збирання метрик"]

    subgraph cluster["Кластер сервісу з Kurin [Software System]"]
        direction TB
        subgraph inst1["Екземпляр 1 (лідер у поточному term)"]
            app1["<b>Application Instance</b><br/>[Container: Java 21, Spring Boot 3.2–4.x, Kurin 1.1.2]<br/>Бізнес-код + вбудований вузол Raft,<br/>блокування, задачі, кеш L1, маршрутизатор БД"]
            raft1[("<b>Raft Storage</b><br/>[Container: файлова система, том PV]<br/>segment-*.log, log.meta,<br/>metadata.bin, snapshot.bin")]
            l2a[("<b>L2 Cache Store</b><br/>[Container: RocksDB 8.8, LZ4, том PV]<br/>записи кешу, invalidation.seq")]
        end
        subgraph inst2["Екземпляри 2..N (фоловери)"]
            app2["<b>Application Instance</b><br/>[Container: той самий образ]"]
            raft2[("<b>Raft Storage</b><br/>[файлова система]")]
            l2b[("<b>L2 Cache Store</b><br/>[RocksDB]")]
        end
    end

    pgp[("<b>PostgreSQL Primary</b><br/>[Container_Ext: PostgreSQL 16]")]
    pgr[("<b>PostgreSQL Replica 1..M</b><br/>[Container_Ext: PostgreSQL 16, hot standby]")]
    l3[("<b>External Cache Store</b><br/>[Container_Ext: Redis 7]<br/>рівень L3")]

    client -- "HTTPS, JWT Bearer" --> gw
    ops -- "HTTPS, JWT Bearer:<br/>GET /actuator/health, /actuator/kurin" --> gw
    gw -- "HTTP/1.1 :8080,<br/>X-Kurin-Consistency" --> app1
    gw -- "HTTP/1.1 :8080,<br/>X-Kurin-Consistency" --> app2
    app1 -- "Raft RPC: AppendEntries, InstallSnapshot, heartbeat<br/>TCP kurin.node.port, mTLS, кадри Kryo" --> app2
    app2 -- "Raft RPC: PreVote, RequestVote, ClientCommand, ClientQuery<br/>TCP kurin.node.port, mTLS, кадри Kryo" --> app1
    app1 -- "append + fsync, атомарне перейменування<br/>Java NIO FileChannel" --> raft1
    app2 -- "Java NIO FileChannel" --> raft2
    app1 -- "get / put / delete<br/>RocksDB JNI" --> l2a
    app2 -- "RocksDB JNI" --> l2b
    app1 -- "запис, pg_current_wal_lsn()<br/>JDBC, TCP 5432, TLS" --> pgp
    app1 -- "читання, pg_last_wal_replay_lsn()<br/>JDBC, TCP 5432, TLS" --> pgr
    app2 -- "JDBC, TCP 5432, TLS" --> pgp
    app2 -- "JDBC, TCP 5432, TLS" --> pgr
    pgp -- "WAL streaming replication<br/>TCP 5432" --> pgr
    app1 -- "get / put / evict<br/>RESP, TCP 6379, TLS" --> l3
    app2 -- "RESP, TCP 6379, TLS" --> l3
    prom -- "HTTP GET /actuator/prometheus<br/>management port :8081" --> app1
    prom -- "HTTP GET /actuator/prometheus" --> app2

    classDef person fill:#08427b,stroke:#052e56,color:#fff
    classDef container fill:#1168bd,stroke:#0b4884,color:#fff
    classDef store fill:#438dd5,stroke:#2e6295,color:#fff
    classDef ext fill:#999999,stroke:#6b6b6b,color:#fff
    class client,ops person
    class app1,app2 container
    class raft1,raft2,l2a,l2b store
    class gw,prom,pgp,pgr,l3 ext
```

**Опис контейнерів і зв'язків рівня 2**

| Контейнер | Технологія | Відповідальність | Трасування |
| :--- | :--- | :--- | :--- |
| Application Instance | Java 21, Spring Boot 3.2–4.x, Kurin 1.1.2 starters; образ OCI, один процес JVM | Бізнес-код і вбудований вузол Raft: вибори, реплікація, автомати блокувань і задач, читання кешу, вибір вузла БД. Декомпозицію наведено в [2.2](#22-компонентне-моделювання-критичного-сервісу-c4-level-3). | [SRS-FUN-01](./srs.md#srs-fun-01) … [SRS-FUN-33](./srs.md#srs-fun-33); [ADR-001](./adr/0001-choice-of-technology.md) |
| Raft Storage | Локальна файлова система (PersistentVolume для кожного екземпляра) | Сегменти журналу з CRC32, база журналу, term і голос, останній снапшот. Не спільна між екземплярами. | [SRS-FUN-05](./srs.md#srs-fun-05), [SRS-FUN-09](./srs.md#srs-fun-09) |
| L2 Cache Store | RocksDB 8.8 (вбудована, LZ4) на локальному томі | Записи кешу, що переживають перезапуск; номер останньої застосованої інвалідації. | [SRS-FUN-19](./srs.md#srs-fun-19), [SRS-FUN-23](./srs.md#srs-fun-23) |
| PostgreSQL Primary / Replica | PostgreSQL 16, потокова реплікація | Дані застосунку; маршрутизатор читає позиції WAL для маркера узгодженості. | [SRS-FUN-24](./srs.md#srs-fun-24) … [SRS-FUN-28](./srs.md#srs-fun-28) |
| External Cache Store | Redis 7 (приклад реалізації `ExternalCacheStore`) | Рівень L3 зі спільним вмістом; запис через впорядковані смуги, вимикач (circuit breaker). | [SRS-FUN-19](./srs.md#srs-fun-19) |
| Ingress / API Gateway | NGINX Ingress або Spring Cloud Gateway | TLS клієнтів, перевірка JWT (OAuth2 / OIDC), rate limiting, балансування між екземплярами. Поза межами Kurin. | [SRS-CON-01](./srs.md#srs-con-01), [SRS-CON-07](./srs.md#srs-con-07) |
| Prometheus | Prometheus 2.x | Pull-збирання метрик `kurin.*`. | [SRS-FUN-32](./srs.md#srs-fun-32) |

| Зв'язок | Протокол | Примітка |
| :--- | :--- | :--- |
| Лідер → фоловер | Raft RPC поверх TCP, TLS 1.2/1.3 з mTLS, кадр `magic\|type\|correlationId\|length\|Kryo` | `AppendEntries` (до 512 записів), `InstallSnapshot` частинами, heartbeat кожні 100 мс; [ADR-001](./adr/0001-choice-of-technology.md), [SRS-NFR-SEC-01](./srs.md#srs-nfr-sec-01) |
| Фоловер → лідер | те саме з'єднання-тип | `PreVote`, `RequestVote` (від кандидата), `ClientCommand` / `ClientQuery` – пересилання роботи застосунку [SRS-FUN-08](./srs.md#srs-fun-08) |
| Екземпляр → Raft Storage | Java NIO `FileChannel`, `force()` | дописування й `fsync` до підтвердження; `.tmp` → атомарне перейменування для метаданих і снапшота |
| Екземпляр → L2 | RocksDB JNI, у процесі | без мережі |
| Екземпляр → PostgreSQL | JDBC, PostgreSQL protocol, TCP 5432, TLS | вибір вузла – у першому SQL-запиті транзакції [SRS-FUN-24](./srs.md#srs-fun-24) |
| Екземпляр → L3 | RESP, TCP 6379, TLS (визначає реалізація) | асинхронні впорядковані записи |
| Gateway → екземпляр | HTTP/1.1, заголовок `X-Kurin-Consistency` | маркер підписано HMAC-SHA256 [SRS-NFR-SEC-03](./srs.md#srs-nfr-sec-03) |
| Prometheus → екземпляр | HTTP GET `/actuator/prometheus` на порту керування | порт керування закрито від клієнтів |

---

## 2.2. Компонентне моделювання критичного сервісу (C4 Level 3)

Для декомпозиції обрано контейнер **Application Instance**, а в ньому – вбудований вузол Kurin: він несе все
координаційне й обчислювальне навантаження (вибори, реплікація, застосування команд, кеш, маршрутизація).
Бізнес-код застосунку показано одним компонентом.

<a id="diagram-level3"></a>

```mermaid
flowchart TB
    peer["Інший Application Instance<br/>[Container]"]
    gwc["Ingress / Gateway<br/>[Container_Ext]"]
    biz["<b>Бізнес-код застосунку</b><br/>[Component: Spring beans]"]

    subgraph inbound["Вхідний шар (Inbound / Adapters)"]
        direction LR
        server["<b>NettyServer</b><br/>[Netty ServerBootstrap]<br/>TCP-порт вузла"]
        tls["<b>TLS Guard</b><br/>[Netty SslHandler, ClientAuth.REQUIRE]<br/>mTLS"]
        hello["<b>Handshake Guard</b><br/>[NettyHandler]<br/>Hello: cluster, версія, відбиток типів, HMAC"]
        codecIn["<b>Frame Codec</b><br/>[MessageDecoder / MessageEncoder]<br/>magic, maxPayloadSize, Kryo"]
        aspects["<b>Annotation Aspects</b><br/>[Spring AOP]<br/>KurinLocksAspect, KurinCacheAspect,<br/>KurinRoutingAspect"]
        validators["<b>Annotation Validators</b><br/>[BeanPostProcessor]<br/>помилка старту за неправильної анотації"]
        actuator["<b>KurinEndpoint + Health Indicators</b><br/>[Spring Boot Actuator]"]
    end

    subgraph core["Шар бізнес-логіки (Core / Domain)"]
        direction LR
        facade["<b>KurinNode</b><br/>[Facade]<br/>submitCommand, submitQuery, leadership()"]
        dispatcher["<b>RaftDispatcher</b><br/>[MessageDispatcher, sealed switch]<br/>передає роботу в потік Raft"]
        raft["<b>RaftState + ролі</b><br/>[State pattern, NodeRole]<br/>Follower / Candidate / Leader / StorageFailed<br/>потік kurin-raft-port"]
        psm["<b>PojoStateMachine</b><br/>[StateMachine]<br/>маршрутизація команд за класом, дедуплікація"]
        services["<b>Replicated services</b><br/>[@KurinService]<br/>LockStateMachine, JobStateMachine,<br/>CacheInvalidationStateMachine"]
        apis["<b>Coordination & Cache API</b><br/>KurinLockManager, KurinLeaderTasks,<br/>KurinJobs, CacheReader"]
        errors["<b>Error Mapper</b><br/>[RpcResult, KurinException hierarchy]<br/>типізовані помилки через мережу"]
    end

    subgraph outbound["Шар інфраструктури (Outbound / Ports & Adapters)"]
        direction LR
        guarded["<b>GuardedStorage</b><br/>[Decorator]<br/>IOException → STORAGE_FAILED"]
        rlog["<b>FileRaftLog</b><br/>[RaftLog]<br/>сегменти, CRC32, group commit"]
        snap["<b>FileSnapshotStore,<br/>FilePersistentMetadata</b><br/>[SnapshotStore, PersistentMetadata]"]
        transport["<b>ConnectionManager + NettyClient</b><br/>[KurinTransport]<br/>пауза перепідключення, PendingRequestsTracker"]
        tiers["<b>Cache Tiers</b><br/>[CacheTier]<br/>CaffeineTier, RocksDbTier, ExternalTier + CircuitBreaker"]
        router["<b>KurinRoutingDataSource</b><br/>[AbstractRoutingDataSource]<br/>+ DataSourceHealthMonitor, NodeProbe"]
        obs["<b>Observability Bridges</b><br/>[KurinObserver, KurinInstrumentation]<br/>Micrometer, Observation"]
    end

    rfs[("Raft Storage")]
    rocks[("L2 Cache Store")]
    redis[("External Cache Store")]
    pg[("PostgreSQL")]

    peer -- "TCP, mTLS" --> server
    server --> tls
    tls -- "сертифікат перевірено" --> hello
    hello -- "Hello прийнято" --> codecIn
    codecIn -- "RaftRpc (record)" --> dispatcher
    gwc -- "HTTP" --> biz
    biz -- "виклик методу через proxy" --> aspects
    aspects -- "інтерфейсний виклик" --> apis
    aspects -- "routing context" --> router
    gwc -- "HTTP /actuator" --> actuator
    actuator -- "KurinNodeHealth.of()" --> facade
    validators -. "перевірка під час старту" .-> aspects
    apis -- "submitCommand / submitQuery" --> facade
    facade -- "post task" --> dispatcher
    dispatcher -- "виклик у потоці Raft" --> raft
    raft -- "StateMachine.apply()" --> psm
    psm -- "@KurinCommand, KurinCommandContext" --> services
    services -. "release / applied listeners (Observer)" .-> apis
    raft -- "outcome" --> errors
    errors -- "RpcResult" --> codecIn
    raft -- "RaftLog, SnapshotStore, PersistentMetadata (порти)" --> guarded
    guarded --> rlog
    guarded --> snap
    raft -- "KurinTransport (порт)" --> transport
    transport -- "TCP, mTLS, Kryo" --> peer
    apis -- "CacheTier (порт)" --> tiers
    raft -. "KurinObserver events" .-> obs
    rlog --> rfs
    snap --> rfs
    tiers -- "RocksDB JNI" --> rocks
    tiers -- "ExternalCacheStore (SPI)" --> redis
    router -- "JDBC" --> pg

    classDef comp fill:#85bbf0,stroke:#5d82a8,color:#000
    classDef guard fill:#f5b7b1,stroke:#c0392b,color:#000
    classDef ext fill:#999999,stroke:#6b6b6b,color:#fff
    class server,codecIn,aspects,actuator,facade,dispatcher,raft,psm,services,apis,rlog,snap,transport,tiers,router,obs,biz comp
    class tls,hello,validators,guarded,errors guard
    class peer,gwc,rfs,rocks,redis,pg ext
```

Червоним виділено компоненти захисту, валідації й обробки помилок.

**Опис компонентів**

| Шар | Компонент | Роль | Взаємодія | Трасування |
| :--- | :--- | :--- | :--- | :--- |
| Inbound | NettyServer | Приймає TCP-з'єднання вузлів-партнерів на `kurin.node.port`. | Netty pipeline | [ADR-001](./adr/0001-choice-of-technology.md) |
| Inbound | TLS Guard | `SslHandler` з `ClientAuth.REQUIRE`: з'єднання без сертифіката, підписаного довіреним центром, закривається до обміну даними. | Netty handler | [SRS-NFR-SEC-01](./srs.md#srs-nfr-sec-01) |
| Inbound | Handshake Guard | Першим повідомленням очікує `Hello`; порівнює ім'я кластера, версію протоколу, відбиток реєстру типів і HMAC-SHA256 над `clusterName\|nodeId\|nonce`; за розбіжності відповідає `HELLO_REJECT` з причиною. | Netty handler | [SRS-FUN-11](./srs.md#srs-fun-11) |
| Inbound | Frame Codec | Перевіряє `magic`, відхиляє кадр понад `maxPayloadSize`, десеріалізує лише зареєстровані типи Kryo. | Netty codec | [SRS-NFR-SEC-02](./srs.md#srs-nfr-sec-02), [SRS-NFR-RES-01](./srs.md#srs-nfr-res-01) |
| Inbound | Annotation Aspects | Перехоплюють методи з `@KurinDistributedLock`, `@KurinLeaderOnly`, `@KurinCacheable`, `@KurinCacheEvict`, `@KurinReadOnly`, `@KurinPrimary` і делегують API ядра; `KurinRoutingAspect` упорядковано перед транзакційним перехоплювачем. | Spring AOP proxy | [SRS-FUN-13](./srs.md#srs-fun-13), [SRS-FUN-19](./srs.md#srs-fun-19), [SRS-FUN-25](./srs.md#srs-fun-25) |
| Inbound | Annotation Validators | `KurinCoordinationRegistry`, `KurinCacheAnnotationValidator`: перевіряють анотації під час створення бінів. | `BeanPostProcessor` | [SRS-FUN-31](./srs.md#srs-fun-31) |
| Inbound | KurinEndpoint + Health | `/actuator/kurin` і health indicators; вердикт обчислюють класи без Spring (`KurinNodeHealth`), адаптери Boot 3 / 4 лише перекладають його. | Actuator | [SRS-FUN-33](./srs.md#srs-fun-33) |
| Core | KurinNode | Фасад вузла: `submitCommand`, `submitQuery`, `leadership()`, `status()`. | Java API | [SRS-FUN-03](./srs.md#srs-fun-03), [SRS-FUN-07](./srs.md#srs-fun-07) |
| Core | RaftDispatcher | Вичерпний `switch` за sealed-інтерфейсом `RaftRpc`; кожну роботу ставить у чергу потоку Raft. | `MessageDispatcher` | [SRS-CON-04](./srs.md#srs-con-04) |
| Core | RaftState + ролі | Однопотоковий стан вузла; ролі – патерн State (`NodeRole`); усі переходи в фоловера – через `stepDown`. | виклики в потоці Raft | [SRS-FUN-01](./srs.md#srs-fun-01) … [SRS-FUN-12](./srs.md#srs-fun-12) |
| Core | PojoStateMachine | Знаходить метод `@KurinCommand` / `@KurinQuery` за класом повідомлення; веде таблицю дедуплікації; збирає снапшот із `@KurinSnapshot` сервісів. | `StateMachine` | [SRS-FUN-04](./srs.md#srs-fun-04) |
| Core | Replicated services | Детерміновані автомати; час і маркер огородження – лише з `KurinCommandContext`. | DI через реєстрацію сервісів | [SRS-FUN-13](./srs.md#srs-fun-13), [SRS-FUN-18](./srs.md#srs-fun-18), [SRS-FUN-21](./srs.md#srs-fun-21) |
| Core | Coordination & Cache API | Очікування звільнення, компенсація невідомого результату, single flight, епохи інвалідацій. | інтерфейсні виклики; Observer від сервісів | [SRS-FUN-15](./srs.md#srs-fun-15), [SRS-FUN-16](./srs.md#srs-fun-16), [SRS-FUN-20](./srs.md#srs-fun-20), [SRS-FUN-22](./srs.md#srs-fun-22) |
| Core | Error Mapper | Усі публічні помилки – підкласи `KurinException`; через мережу передаються в `RpcResult(ok, value, errorType, errorMessage, leaderHint)`. | Global error handler протоколу | [SRS-FUN-09](./srs.md#srs-fun-09), [SRS-FUN-15](./srs.md#srs-fun-15) |
| Outbound | GuardedStorage | Декоратор над портами сховища: `UncheckedIOException` переводить вузол у `STORAGE_FAILED`. | Decorator | [SRS-FUN-09](./srs.md#srs-fun-09), [SRS-FUN-10](./srs.md#srs-fun-10) |
| Outbound | FileRaftLog | Реалізація порту `RaftLog`: сегменти по 8 МіБ, запис `length\|crc32\|Kryo(LogEntry)`, група записів на один `fsync`. | `RaftLog` | [SRS-FUN-05](./srs.md#srs-fun-05), [SRS-NFR-SCAL-02](./srs.md#srs-nfr-scal-02) |
| Outbound | FileSnapshotStore, FilePersistentMetadata | Снапшот і term/голос через `.tmp` → `fsync` → атомарне перейменування. | `SnapshotStore`, `PersistentMetadata` | [SRS-FUN-06](./srs.md#srs-fun-06) |
| Outbound | ConnectionManager + NettyClient | Одне вихідне з'єднання на вузол; пауза після невдалої спроби; запити протоколу Raft під час паузи завершуються одразу, пересилання роботи застосунку чекає наступної спроби. | `KurinTransport` | [SRS-FUN-08](./srs.md#srs-fun-08) |
| Outbound | Cache Tiers | L1 Caffeine, L2 RocksDB, L3 `ExternalTier` з вимикачем і впорядкованими записами за смугами ключів. | `CacheTier`, SPI `ExternalCacheStore` | [SRS-FUN-19](./srs.md#srs-fun-19) |
| Outbound | KurinRoutingDataSource | За `LazyConnectionDataSourceProxy`; монітор здоров'я з `NodeProbe` і класифікатором вичерпаного пулу. | `DataSource`, SPI `ShardResolver`, `ReplicaPositionProbe` | [SRS-FUN-24](./srs.md#srs-fun-24) … [SRS-FUN-29](./srs.md#srs-fun-29) |
| Outbound | Observability Bridges | Події `KurinObserver` → лічильники й таймери Micrometer; `KurinInstrumentation` → Observation (спани передаються лідеру в заголовках запиту). | Observer | [SRS-FUN-32](./srs.md#srs-fun-32) |

**Дотримання шарів і принципів.** Вхідний шар не звертається до сховищ: кадр проходить TLS → рукостискання →
кодек → `RaftDispatcher` → `RaftState`. Ядро залежить від портів (`RaftLog`, `SnapshotStore`,
`PersistentMetadata`, `KurinTransport`, `CacheTier`), а не від файлів і Netty (Dependency Inversion); реалізації
підставляються під час збирання вузла (`KurinNode.builder()`), тому `kurin-test` замінює транспорт і сховище
без зміни ядра. Автомати станів не знають про мережу й диск. Зворотні сповіщення від автоматів до API (звільнення
блокування, застосування інвалідації) – патерн Observer, щоб ядро не залежало від клієнтських класів.

---

## 2.3. Документування архітектурних рішень (Architecture Decision Records)

| ADR | Рішення | Статус | Зачеплені контейнери / компоненти |
| :--- | :--- | :--- | :--- |
| [ADR-001](./adr/0001-choice-of-technology.md) | Транспорт і серіалізація протоколу вузлів: Netty + Kryo з реєстром типів замість gRPC + Protocol Buffers | Approved | Application Instance: NettyServer, Handshake Guard, Frame Codec, ConnectionManager |

Нові рішення додаються файлами `docs/adr/NNNN-<назва>.md` за тим самим шаблоном.
