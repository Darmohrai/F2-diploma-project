package org.kurin.kurinlocksspringbootstarter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "kurin.jobs")
public class KurinJobsProperties {
    /**
     * Number of most recent slots remembered per job. Older slots are dropped and can never be claimed again.
     */
    private int historySize = 50;
    /**
     * Time to wait for the cluster to answer a claim or a completion report.
     */
    private Duration commandTimeout = Duration.ofSeconds(3);

    public int getHistorySize() { return historySize; }
    public void setHistorySize(int historySize) { this.historySize = historySize; }

    public Duration getCommandTimeout() { return commandTimeout; }
    public void setCommandTimeout(Duration commandTimeout) { this.commandTimeout = commandTimeout; }
}
