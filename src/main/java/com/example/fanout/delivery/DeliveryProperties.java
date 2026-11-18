package com.example.fanout.delivery;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("fanout.delivery")
public class DeliveryProperties {
    private boolean enabled = true;
    private boolean allowHttp;
    private boolean allowPrivateAddresses;
    @NotNull private Duration timeout = Duration.ofSeconds(10);
    @NotNull private Duration lease = Duration.ofSeconds(60);
    @NotNull private Duration initialBackoff = Duration.ofSeconds(1);
    @NotNull private Duration maxBackoff = Duration.ofMinutes(5);
    @Min(1) @Max(100) private int maxAttempts = 8;
    @Min(1) @Max(128) private int concurrency = 8;

    @AssertTrue(message = "delivery timeout must be 100ms..120s, lease must exceed timeout by at least 5s, and backoff bounds must be valid")
    public boolean isTimingValid() {
        return timeout != null && lease != null && initialBackoff != null && maxBackoff != null
                && timeout.compareTo(Duration.ofMillis(100)) >= 0 && timeout.compareTo(Duration.ofSeconds(120)) <= 0
                && lease.compareTo(timeout.plusSeconds(5)) >= 0 && lease.compareTo(Duration.ofHours(1)) <= 0
                && initialBackoff.compareTo(Duration.ofMillis(1)) >= 0
                && maxBackoff.compareTo(initialBackoff) >= 0 && maxBackoff.compareTo(Duration.ofDays(1)) <= 0;
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isAllowHttp() { return allowHttp; }
    public void setAllowHttp(boolean allowHttp) { this.allowHttp = allowHttp; }
    public boolean isAllowPrivateAddresses() { return allowPrivateAddresses; }
    public void setAllowPrivateAddresses(boolean allowPrivateAddresses) { this.allowPrivateAddresses = allowPrivateAddresses; }
    public Duration getTimeout() { return timeout; }
    public void setTimeout(Duration timeout) { this.timeout = timeout; }
    public Duration getLease() { return lease; }
    public void setLease(Duration lease) { this.lease = lease; }
    public Duration getInitialBackoff() { return initialBackoff; }
    public void setInitialBackoff(Duration initialBackoff) { this.initialBackoff = initialBackoff; }
    public Duration getMaxBackoff() { return maxBackoff; }
    public void setMaxBackoff(Duration maxBackoff) { this.maxBackoff = maxBackoff; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public int getConcurrency() { return concurrency; }
    public void setConcurrency(int concurrency) { this.concurrency = concurrency; }
}
