package org.kurin.kurinspringbootstarter.starter;

import org.kurin.raft.RaftConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "kurin.node")
public class KurinProperties {
    /**
     * Address other nodes use to reach this node; with the port it forms the node id.
     */
    private String host;
    private int port;
    /**
     * Cluster members as host:port. May include this node.
     */
    private List<String> peers = new ArrayList<>();
    private RaftConfig.StorageType storage = RaftConfig.StorageType.FILE;
    /**
     * Directory of the durable node state. Defaults to ./kurin-data/<host>-<port>.
     */
    private Path dataDir;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public List<String> getPeers() { return peers; }
    public void setPeers(List<String> peers) { this.peers = peers; }

    public RaftConfig.StorageType getStorage() { return storage; }
    public void setStorage(RaftConfig.StorageType storage) { this.storage = storage; }

    public Path getDataDir() { return dataDir; }
    public void setDataDir(Path dataDir) { this.dataDir = dataDir; }
}
