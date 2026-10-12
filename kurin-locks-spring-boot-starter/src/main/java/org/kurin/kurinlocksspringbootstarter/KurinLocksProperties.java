package org.kurin.kurinlocksspringbootstarter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "kurin.locks")
public class KurinLocksProperties {
    /**
     * Time to wait for the cluster to answer a single lock command (acquire, renew, release).
     */
    private Duration commandTimeout = Duration.ofSeconds(3);
    /**
     * Interval at which the leader removes expired lock records.
     */
    private Duration purgeInterval = Duration.ofSeconds(30);

    public Duration getCommandTimeout() { return commandTimeout; }
    public void setCommandTimeout(Duration commandTimeout) { this.commandTimeout = commandTimeout; }

    public Duration getPurgeInterval() { return purgeInterval; }
    public void setPurgeInterval(Duration purgeInterval) { this.purgeInterval = purgeInterval; }
}
