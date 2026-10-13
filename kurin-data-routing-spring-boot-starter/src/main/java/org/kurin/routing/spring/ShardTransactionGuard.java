package org.kurin.routing.spring;

import org.kurin.routing.KurinCrossShardTransactionException;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Pins a transaction to the first shard it touches and refuses any other shard until it ends.
 */
final class ShardTransactionGuard {
    private static final Object SHARD_OF_TRANSACTION = new Object() {
        @Override
        public String toString() {
            return "kurin.shardOfTransaction";
        }
    };

    private ShardTransactionGuard() {
    }

    /**
     * Notes that the current transaction, if there is one, uses the shard.
     *
     * @throws KurinCrossShardTransactionException if the transaction already uses another shard
     */
    static void enter(String shard) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()
                || !TransactionSynchronizationManager.isActualTransactionActive()) {
            return;
        }
        Object bound = TransactionSynchronizationManager.getResource(SHARD_OF_TRANSACTION);
        if (bound == null) {
            TransactionSynchronizationManager.bindResource(SHARD_OF_TRANSACTION, shard);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCompletion(int status) {
                    TransactionSynchronizationManager.unbindResourceIfPossible(SHARD_OF_TRANSACTION);
                }

                // A suspended transaction (REQUIRES_NEW inside it) must not pin the inner one
                @Override
                public void suspend() {
                    TransactionSynchronizationManager.unbindResourceIfPossible(SHARD_OF_TRANSACTION);
                }

                @Override
                public void resume() {
                    TransactionSynchronizationManager.bindResource(SHARD_OF_TRANSACTION, shard);
                }
            });
        } else if (!bound.equals(shard)) {
            throw new KurinCrossShardTransactionException((String) bound, shard);
        }
    }
}
