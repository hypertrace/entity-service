package org.hypertrace.entity.query.service.client;

public class EntityLabelsCachingClientConfig {
  private final long maximumSize;
  private final long expiryTimeInMinutes;

  public EntityLabelsCachingClientConfig(long maximumSize, long expiryTimeInMinutes) {
    this.maximumSize = maximumSize;
    this.expiryTimeInMinutes = expiryTimeInMinutes;
  }

  public long getMaximumSize() {
    return maximumSize;
  }

  public long getExpiryTimeInMinutes() {
    return expiryTimeInMinutes;
  }
}
