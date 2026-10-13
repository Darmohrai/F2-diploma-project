package org.kurin.it;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * A small application that uses every module of Kurin: a replicated counter, a cluster-wide lock, a job that
 * runs on one instance per slot, a cache with replicated invalidation and a database split into two shards with
 * a replica each. The integration tests start three instances of it in one JVM.
 */
@SpringBootApplication
public class ItApplication {
}
