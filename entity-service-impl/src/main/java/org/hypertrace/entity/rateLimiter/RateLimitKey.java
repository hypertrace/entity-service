package org.hypertrace.entity.rateLimiter;

import java.util.Optional;
import lombok.Value;

@Value
public class RateLimitKey {
  String tenantId;
  Optional<String> environment;
  String entityType;
}
