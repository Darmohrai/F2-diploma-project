package org.kurin.kurinlocksspringbootstarter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kurin.locks")
public class KurinLocksProperties {
    private long lockTimeoutMs = 3000;
    private long unlockTimeoutMs = 2000;

    public long getLockTimeoutMs() { return lockTimeoutMs; }
    public void setLockTimeoutMs(long lockTimeoutMs) { this.lockTimeoutMs = lockTimeoutMs; }

    public long getUnlockTimeoutMs() { return unlockTimeoutMs; }
    public void setUnlockTimeoutMs(long unlockTimeoutMs) { this.unlockTimeoutMs = unlockTimeoutMs; }
}
