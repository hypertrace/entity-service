package org.hypertrace.entity.rateLimiter;

import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class EntityRateLimiterCacheConfig {
  private final Duration refreshDuration;
  private final Duration expiryDuration;
  private final int maxSize;
}
