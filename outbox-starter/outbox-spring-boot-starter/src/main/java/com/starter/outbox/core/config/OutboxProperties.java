package com.starter.outbox.core.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration properties for the outbox starter.
 *
 * <pre>
 * outbox:
 *   enabled: true
 *   batch-size: 50
 *   poll-interval-ms: 1000
 *   default-max-retries: 5
 *   retry-base-delay: 5s
 *   retry-max-backoff: 1h
 *   archive:
 *     enabled: true
 *     retention: 7d
 *     cron: "0 0 3 * * *"
 * </pre>
 */
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {

    /** Master switch — if false, the dispatcher does not start. */
    private boolean enabled = true;

    /** Number of events fetched per dispatch cycle. */
    private int batchSize = 50;

    /** Polling interval in milliseconds. */
    private long pollIntervalMs = 1000;

    /** Default max retry count (can be overridden per-event). */
    private int defaultMaxRetries = 5;

    /** Base delay for exponential backoff. */
    private Duration retryBaseDelay = Duration.ofSeconds(5);

    /** Maximum backoff cap. */
    private Duration retryMaxBackoff = Duration.ofHours(1);

    /** Age threshold after which RETRYING events are force-dead-lettered. */
    private Duration staleRetryThreshold = Duration.ofDays(1);

    /** Archive / cleanup settings. */
    private Archive archive = new Archive();

    // ── nested archive config ────────────────────────────────────────

    public static class Archive {
        private boolean enabled = true;
        private Duration retention = Duration.ofDays(7);
        private String cron = "0 0 3 * * *"; // 3 AM daily

        public boolean isEnabled()       { return enabled; }
        public void setEnabled(boolean v){ this.enabled = v; }
        public Duration getRetention()   { return retention; }
        public void setRetention(Duration v) { this.retention = v; }
        public String getCron()          { return cron; }
        public void setCron(String v)    { this.cron = v; }
    }

    // ── getters / setters ────────────────────────────────────────────

    public boolean isEnabled()                      { return enabled; }
    public void setEnabled(boolean enabled)         { this.enabled = enabled; }
    public int getBatchSize()                       { return batchSize; }
    public void setBatchSize(int batchSize)         { this.batchSize = batchSize; }
    public long getPollIntervalMs()                 { return pollIntervalMs; }
    public void setPollIntervalMs(long v)           { this.pollIntervalMs = v; }
    public int getDefaultMaxRetries()               { return defaultMaxRetries; }
    public void setDefaultMaxRetries(int v)         { this.defaultMaxRetries = v; }
    public Duration getRetryBaseDelay()             { return retryBaseDelay; }
    public void setRetryBaseDelay(Duration v)       { this.retryBaseDelay = v; }
    public Duration getRetryMaxBackoff()            { return retryMaxBackoff; }
    public void setRetryMaxBackoff(Duration v)      { this.retryMaxBackoff = v; }
    public Duration getStaleRetryThreshold()        { return staleRetryThreshold; }
    public void setStaleRetryThreshold(Duration v)  { this.staleRetryThreshold = v; }
    public Archive getArchive()                     { return archive; }
    public void setArchive(Archive archive)         { this.archive = archive; }
}
