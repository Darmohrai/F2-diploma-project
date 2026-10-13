package org.kurin.routing;

/**
 * Code running in a transaction on one shard tried to reach another shard. A local transaction lives on one
 * database connection and cannot span shards: the second access would silently run on the first shard, or
 * outside of the transaction. Split the work into one transaction per shard.
 */
public class KurinCrossShardTransactionException extends KurinRoutingException {
    public KurinCrossShardTransactionException(String transactionShard, String requestedShard) {
        super("The current transaction runs on shard '" + transactionShard + "' and cannot also use shard '"
                + requestedShard + "'. A local transaction cannot span shards.");
    }
}
