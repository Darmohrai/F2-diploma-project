package org.kurin.kurincachespringbootstarter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kurin.routing")
public class KurinRoutingProperties {
    private long stickyWindowMs = 3000;
    private int virtualNodes = 160;

    public long getStickyWindowMs() { return stickyWindowMs; }
    public void setStickyWindowMs(long stickyWindowMs) { this.stickyWindowMs = stickyWindowMs; }

    public int getVirtualNodes() { return virtualNodes; }
    public void setVirtualNodes(int virtualNodes) { this.virtualNodes = virtualNodes; }
}
