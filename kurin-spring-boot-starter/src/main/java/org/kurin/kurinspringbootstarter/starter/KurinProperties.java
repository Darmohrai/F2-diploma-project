package org.kurin.kurinspringbootstarter.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "kurin.node")
public class KurinProperties {
    private String host = "127.0.0.1";
    private int port;
    private List<String> peers = new ArrayList<>();

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public List<String> getPeers() { return peers; }
    public void setPeers(List<String> peers) { this.peers = peers; }
}
