package org.kurin.kurinspringbootstarter.observability;

import org.kurin.raft.KurinNode;
import org.kurin.raft.RaftStatus;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only view of the Kurin node of this instance and of the modules built on it, at {@code /actuator/kurin}.
 */
@Endpoint(id = "kurin")
public class KurinEndpoint {
    private final KurinNode node;
    private final ObjectProvider<KurinEndpointContributor> contributors;

    public KurinEndpoint(KurinNode node, ObjectProvider<KurinEndpointContributor> contributors) {
        this.node = node;
        this.contributors = contributors;
    }

    @ReadOperation
    public Map<String, Object> kurin() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("node", org.kurin.raft.KurinNodeHealth.describe(node));
        contributors.orderedStream().forEach(contributor -> {
            try {
                response.put(contributor.name(), contributor.describe());
            } catch (RuntimeException e) {
                response.put(contributor.name(), Map.of("error", String.valueOf(e)));
            }
        });
        return response;
    }

}
