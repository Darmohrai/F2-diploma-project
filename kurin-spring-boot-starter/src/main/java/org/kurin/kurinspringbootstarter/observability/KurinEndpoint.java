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
        response.put("node", describeNode(node));
        contributors.orderedStream().forEach(contributor -> {
            try {
                response.put(contributor.name(), contributor.describe());
            } catch (RuntimeException e) {
                response.put(contributor.name(), Map.of("error", String.valueOf(e)));
            }
        });
        return response;
    }

    static Map<String, Object> describeNode(KurinNode node) {
        RaftStatus status = node.status();
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("nodeId", status.nodeId());
        details.put("started", node.isStarted());
        details.put("role", status.role().name());
        details.put("term", status.term());
        details.put("leaderId", status.leaderId());
        details.put("leadershipEstablished", node.leadership().isPresent());
        details.put("commitIndex", status.commitIndex());
        details.put("lastApplied", status.lastApplied());
        details.put("lastLogIndex", status.lastLogIndex());
        details.put("logEntries", node.logEntries());
        if (status.storageFailed()) {
            details.put("storageFailure", status.storageFailure());
        }

        Map<String, Object> peers = new LinkedHashMap<>();
        Map<String, java.time.Duration> contacts = node.peerContactAges();
        node.peers().forEach(peer -> {
            java.time.Duration age = contacts.get(peer.asString());
            peers.put(peer.asString(), age != null ? Map.of("lastContactMsAgo", age.toMillis()) : Map.of("lastContactMsAgo", "unknown"));
        });
        details.put("peers", peers);
        return details;
    }
}
