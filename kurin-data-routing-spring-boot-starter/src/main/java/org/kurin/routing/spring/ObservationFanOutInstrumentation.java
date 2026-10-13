package org.kurin.routing.spring;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.kurin.routing.shard.KurinShardOperations.FanOutInstrumentation;
import org.kurin.routing.shard.KurinShardOperations.FanOutSpan;
import org.kurin.routing.shard.KurinShardOperations.ShardSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports a fan-out over the shards as Micrometer observations: {@code kurin.routing.fanout} for the whole call,
 * tagged with the number of shards and of failed shards, and {@code kurin.routing.fanout.shard} for the work on
 * each shard, tagged with the shard. The first continues whatever the caller was observed in; each of the
 * second is its child and is current on the thread that works on the shard, so that spans of the statements run
 * there hang below it. A failing tracing system is logged and never fails the fan-out.
 */
public class ObservationFanOutInstrumentation implements FanOutInstrumentation {
    private static final Logger log = LoggerFactory.getLogger(ObservationFanOutInstrumentation.class);
    private static final ShardSpan NO_SHARD_SPAN = error -> {
    };

    private final ObservationRegistry registry;

    public ObservationFanOutInstrumentation(ObservationRegistry registry) {
        this.registry = registry;
    }

    @Override
    public FanOutSpan start(int shards) {
        Observation fanOut;
        try {
            // Started on the calling thread: the observation current there becomes its parent
            fanOut = Observation.createNotStarted("kurin.routing.fanout", registry)
                    .lowCardinalityKeyValue("shards", Integer.toString(shards))
                    .start();
        } catch (RuntimeException e) {
            log.debug("Observation of a fan-out could not be started: {}", e.toString());
            return NOOP.start(shards);
        }
        return new FanOutSpan() {
            @Override
            public ShardSpan shard(String shard) {
                try {
                    Observation work = Observation.createNotStarted("kurin.routing.fanout.shard", registry)
                            .parentObservation(fanOut)
                            .lowCardinalityKeyValue("shard", shard)
                            .start();
                    Observation.Scope scope = work.openScope();
                    return error -> {
                        try {
                            scope.close();
                            if (error != null) {
                                work.error(error);
                            }
                            work.stop();
                        } catch (RuntimeException e) {
                            log.debug("Observation of shard '{}' could not be ended: {}", shard, e.toString());
                        }
                    };
                } catch (RuntimeException e) {
                    log.debug("Observation of shard '{}' could not be started: {}", shard, e.toString());
                    return NO_SHARD_SPAN;
                }
            }

            @Override
            public void end(int failed) {
                try {
                    fanOut.lowCardinalityKeyValue("failed", Integer.toString(failed));
                    fanOut.stop();
                } catch (RuntimeException e) {
                    log.debug("Observation of a fan-out could not be ended: {}", e.toString());
                }
            }
        };
    }
}
