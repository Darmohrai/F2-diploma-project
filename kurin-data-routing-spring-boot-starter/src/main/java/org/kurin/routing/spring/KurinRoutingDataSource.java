package org.kurin.routing.spring;

import org.kurin.routing.DataSourceType;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.KurinPrimaryUnavailableException;
import org.kurin.routing.KurinReadOnly;
import org.kurin.routing.KurinRoutingContext;
import org.kurin.routing.KurinRoutingException;
import org.kurin.routing.consistency.ConsistencyTokenStrategy;
import org.kurin.routing.consistency.KurinConsistency;
import org.kurin.routing.health.ConnectionFailureClassifier.FailureKind;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Picks the physical database for every connection request: the shard from the routing context, then primary
 * or replica.
 * <p>
 * Rules for primary versus replica, first match wins:
 * <ol>
 *   <li>the primary was demanded ({@code @KurinPrimary}): primary;</li>
 *   <li>a read-write transaction is active: primary, even inside {@code @KurinReadOnly}, and the client is
 *       noted as a writer when the transaction commits;</li>
 *   <li>replicas are allowed ({@code @KurinReadOnly}, or a read-only transaction when that is enabled): replica,
 *       unless read-your-writes says this client must still read from the primary;</li>
 *   <li>otherwise: primary.</li>
 * </ol>
 * Then node health applies: a read falls back from a dead replica to the primary (or fails, if so configured),
 * and anything that needs a dead primary fails at once with {@link KurinPrimaryUnavailableException}.
 * <p>
 * The decision needs to know whether the surrounding transaction is read-only, which Spring publishes only
 * after it fetched the connection. Use this class behind a {@code LazyConnectionDataSourceProxy}, as the
 * auto-configuration does, so that the physical connection is requested at the first statement.
 */
public class KurinRoutingDataSource extends AbstractDataSource {
    private static final Logger log = LoggerFactory.getLogger(KurinRoutingDataSource.class);

    public record Route(String shard, DataSourceType role) {
    }

    /**
     * Observes routing decisions, for metrics.
     */
    public interface Listener {
        void onConnection(String shard, DataSourceType role);

        /**
         * A read could not use the node it was routed to and was sent to the primary instead.
         */
        void onFallback(String shard, String reason);
    }

    private final KurinDataSourceTopology topology;
    private final ConsistencyTokenStrategy strategy;
    private final DataSourceHealthMonitor health;
    private final KurinRoutingProperties.ReplicaFallback replicaFallback;
    private final boolean readonlyTransactionsToReplica;
    private final String singleShard;
    private final KurinRoutingProperties.UnroutedMode unroutedMode;
    private final String unroutedShard;
    private final java.util.function.BooleanSupplier startupFinished;
    private final java.util.concurrent.atomic.AtomicBoolean startupAccessExplained = new java.util.concurrent.atomic.AtomicBoolean();
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    public KurinRoutingDataSource(KurinDataSourceTopology topology, ConsistencyTokenStrategy strategy,
                                  DataSourceHealthMonitor health, KurinRoutingProperties.ReplicaFallback replicaFallback,
                                  boolean readonlyTransactionsToReplica) {
        this(topology, strategy, health, replicaFallback, readonlyTransactionsToReplica,
                KurinRoutingProperties.UnroutedMode.FAIL, null, () -> true);
    }

    /**
     * @param unroutedMode    what to do with access that selected no shard, when there are several
     * @param unroutedShard   the shard that serves such access where the mode allows it; null means the first
     * @param startupFinished tells whether the application is ready; consulted in the mode STARTUP_ONLY
     */
    public KurinRoutingDataSource(KurinDataSourceTopology topology, ConsistencyTokenStrategy strategy,
                                  DataSourceHealthMonitor health, KurinRoutingProperties.ReplicaFallback replicaFallback,
                                  boolean readonlyTransactionsToReplica, KurinRoutingProperties.UnroutedMode unroutedMode,
                                  String unroutedShard, java.util.function.BooleanSupplier startupFinished) {
        this.topology = topology;
        this.strategy = strategy;
        this.health = health;
        this.replicaFallback = replicaFallback;
        this.readonlyTransactionsToReplica = readonlyTransactionsToReplica;
        this.singleShard = topology.isSharded() ? null : topology.shards().keySet().iterator().next();
        this.unroutedMode = unroutedMode;
        this.unroutedShard = unroutedShard != null ? unroutedShard : topology.shards().keySet().iterator().next();
        this.startupFinished = startupFinished;
        if (!topology.shards().containsKey(this.unroutedShard)) {
            throw new IllegalStateException("kurin.routing.unrouted.shard=" + this.unroutedShard
                    + " is not a shard of the topology " + topology.shards().keySet());
        }
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connect(route(), null, null);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return connect(route(), username, password);
    }

    // ---------------------------------------------------------------- decision

    /**
     * @return the node the current thread would be routed to right now
     * @throws KurinRoutingException            if no shard is selected in a sharded topology
     * @throws KurinPrimaryUnavailableException if the primary is needed and down
     */
    public Route route() {
        boolean selected = KurinRoutingContext.shard() != null || singleShard != null;
        String shard = selected ? resolveShard() : unroutedShard();
        ShardTransactionGuard.enter(shard);
        Map<String, KurinDataSourceTopology.Shard> shards = topology.shards();
        DataSource replica = shards.get(shard).replica();

        boolean transactionActive = TransactionSynchronizationManager.isActualTransactionActive();
        boolean transactionReadOnly = TransactionSynchronizationManager.isCurrentTransactionReadOnly();
        DataSourceType requested = KurinRoutingContext.requestedType();

        DataSourceType role;
        if (!selected && unroutedMode == KurinRoutingProperties.UnroutedMode.STARTUP_ONLY) {
            // Whatever runs before the application can choose a shard gets the one node that has everything
            role = DataSourceType.PRIMARY;
        } else if (requested == DataSourceType.PRIMARY) {
            role = DataSourceType.PRIMARY;
        } else if (transactionActive && !transactionReadOnly) {
            role = DataSourceType.PRIMARY;
            noteWriterAfterCommit(shard);
        } else if (requested == DataSourceType.REPLICA) {
            role = replicaIsFreshEnough(shard, replica, KurinRoutingContext.consistency()) ? DataSourceType.REPLICA : DataSourceType.PRIMARY;
        } else if (transactionActive && readonlyTransactionsToReplica) {
            role = replicaIsFreshEnough(shard, replica, KurinReadOnly.Consistency.READ_YOUR_WRITES) ? DataSourceType.REPLICA : DataSourceType.PRIMARY;
        } else {
            role = DataSourceType.PRIMARY;
        }

        if (role == DataSourceType.REPLICA && replica == null) {
            role = DataSourceType.PRIMARY;
        }
        if (role == DataSourceType.REPLICA && !health.isUp(shard, DataSourceType.REPLICA)) {
            if (replicaFallback == KurinRoutingProperties.ReplicaFallback.FAIL) {
                throw new KurinRoutingException("The replica of shard '" + shard + "' is unavailable and reads are "
                        + "configured not to fall back to the primary");
            }
            listeners.forEach(listener -> listener.onFallback(shard, "replica-down"));
            role = DataSourceType.PRIMARY;
        }
        if (role == DataSourceType.PRIMARY && !health.isUp(shard, DataSourceType.PRIMARY)) {
            // Writes and reads that must be current cannot be served; eventual reads were routed to the replica above
            throw new KurinPrimaryUnavailableException(shard);
        }
        return new Route(shard, role);
    }

    private static final String HOW_TO_SELECT = "Mark the shard key argument of the method with @KurinShardKey, use "
            + "@KurinSharded, or run it through KurinShardOperations.";

    // The shard for access that selected none, in a topology with several
    private String unroutedShard() {
        switch (unroutedMode) {
            case DEFAULT_SHARD -> {
                log.debug("No shard is selected: using shard '{}' (kurin.routing.unrouted.mode=default-shard)", unroutedShard);
                return unroutedShard;
            }
            case STARTUP_ONLY -> {
                if (startupFinished.getAsBoolean()) {
                    throw new KurinRoutingException("No shard is selected for the current operation. " + HOW_TO_SELECT
                            + " Access without a shard was served by shard '" + unroutedShard + "' only while the application "
                            + "was starting (kurin.routing.unrouted.mode=startup-only).");
                }
                if (startupAccessExplained.compareAndSet(false, true)) {
                    log.info("Database access without a selected shard is served by the primary of shard '{}' while the "
                            + "application starts: schema validation, driver metadata and the like run before any code can "
                            + "choose a shard. Once the application is ready such access fails "
                            + "(kurin.routing.unrouted.mode=startup-only).", unroutedShard);
                } else {
                    log.debug("No shard is selected during startup: using the primary of shard '{}'", unroutedShard);
                }
                return unroutedShard;
            }
            default -> throw new KurinRoutingException("No shard is selected for the current operation. " + HOW_TO_SELECT);
        }
    }

    private String resolveShard() {
        String shard = KurinRoutingContext.shard();
        if (shard == null) {
            shard = singleShard;
        }
        if (!topology.shards().containsKey(shard)) {
            throw new KurinRoutingException("Unknown shard '" + shard + "'. Configured shards: " + topology.shards().keySet());
        }
        return shard;
    }

    private boolean replicaIsFreshEnough(String shard, DataSource replica, KurinReadOnly.Consistency consistency) {
        if (replica == null || consistency == KurinReadOnly.Consistency.EVENTUAL) {
            return true;
        }
        return !strategy.requiresPrimary(KurinConsistency.current().position(shard), shard, replica);
    }

    // Once per transaction and shard: when the transaction commits, the client's token records the write
    private void noteWriterAfterCommit(String shard) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return;
        }
        Object marker = new WriterMark(shard);
        if (TransactionSynchronizationManager.hasResource(marker)) {
            return;
        }
        TransactionSynchronizationManager.bindResource(marker, Boolean.TRUE);
        DataSource primary = topology.shards().get(shard).primary();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    KurinConsistency.recordWrite(shard, strategy.onWriteCommitted(shard, primary));
                } catch (RuntimeException e) {
                    log.warn("The write to shard '{}' could not be noted in the consistency token: {}", shard, e.toString());
                }
            }

            @Override
            public void afterCompletion(int status) {
                TransactionSynchronizationManager.unbindResourceIfPossible(marker);
            }
        });
    }

    private record WriterMark(String shard) {
    }

    // ---------------------------------------------------------------- connecting

    private Connection connect(Route route, String username, String password) throws SQLException {
        KurinDataSourceTopology.Shard shard = topology.shards().get(route.shard());
        DataSource target = route.role() == DataSourceType.REPLICA ? shard.replica() : shard.primary();
        try {
            Connection connection = username != null ? target.getConnection(username, password) : target.getConnection();
            listeners.forEach(listener -> listener.onConnection(route.shard(), route.role()));
            return connection;
        } catch (SQLException | RuntimeException e) {
            // Passive health check: a node that could not be connected to is not used again until the monitor
            // sees it working
            FailureKind kind = health.reportConnectionFailure(route.shard(), route.role(), e);
            if (!kind.nodeUnavailable()) {
                // The pool had no free connection in time. The node is in order and so is the route: handing a read
                // of a busy replica to the primary would only move the load there. The caller gets what a
                // saturated pool gives anyone.
                log.debug("The pool of the {} of shard '{}' has no free connection: {}", route.role(), route.shard(), e.toString());
                throw e;
            }
            if (route.role() == DataSourceType.REPLICA && replicaFallback == KurinRoutingProperties.ReplicaFallback.PRIMARY
                    && health.isUp(route.shard(), DataSourceType.PRIMARY)) {
                log.debug("Replica of shard '{}' refused a connection, reading from the primary: {}", route.shard(), e.toString());
                listeners.forEach(listener -> listener.onFallback(route.shard(), "replica-error"));
                return connect(new Route(route.shard(), DataSourceType.PRIMARY), username, password);
            }
            if (route.role() == DataSourceType.PRIMARY) {
                KurinPrimaryUnavailableException unavailable = new KurinPrimaryUnavailableException(route.shard());
                unavailable.initCause(e);
                throw unavailable;
            }
            throw e;
        }
    }
}
