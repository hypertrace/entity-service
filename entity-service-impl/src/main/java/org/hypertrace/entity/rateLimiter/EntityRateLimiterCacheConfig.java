package org.hypertrace.entity.rateLimiter;

import java.time.Duration;
import lombok.Value;

@Value
public class EntityRateLimiterCacheConfig {
  Duration refreshDuration;
  Duration expiryDuration;
  int maxSize;
}
