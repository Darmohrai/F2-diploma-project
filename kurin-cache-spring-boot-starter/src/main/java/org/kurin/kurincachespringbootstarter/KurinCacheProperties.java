package org.kurin.kurincachespringbootstarter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kurin.cache")
public class KurinCacheProperties {
    private String storageDir = "./kurin-data/cache";
    private long l1MaxSize = 10000;
    private long l1ExpireMinutes = 15;
    private int l3MaxFailures = 3;
    private long l3CircuitOpenTimeoutMs = 10000;

    public String getStorageDir() { return storageDir; }
    public void setStorageDir(String storageDir) { this.storageDir = storageDir; }

    public long getL1MaxSize() { return l1MaxSize; }
    public void setL1MaxSize(long l1MaxSize) { this.l1MaxSize = l1MaxSize; }

    public long getL1ExpireMinutes() { return l1ExpireMinutes; }
    public void setL1ExpireMinutes(long l1ExpireMinutes) { this.l1ExpireMinutes = l1ExpireMinutes; }

    public int getL3MaxFailures() { return l3MaxFailures; }
    public void setL3MaxFailures(int l3MaxFailures) { this.l3MaxFailures = l3MaxFailures; }

    public long getL3CircuitOpenTimeoutMs() { return l3CircuitOpenTimeoutMs; }
    public void setL3CircuitOpenTimeoutMs(long l3CircuitOpenTimeoutMs) { this.l3CircuitOpenTimeoutMs = l3CircuitOpenTimeoutMs; }
}
