package org.hypertrace.entity.query.service.client;

import java.util.Map;
import java.util.Objects;

public class EntityCacheKey<T> {
  private final T dataKey;
  private final String tenantId;
  private final Map<String, String> headers;

  public EntityCacheKey(T key, String tenantId, Map<String, String> headers) {
    this.dataKey = key;
    this.tenantId = tenantId;
    this.headers = headers;
  }

  public T getDataKey() {
    return dataKey;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  // Only dataKey and tenantId are considered as part of the cache key. Headers are not.
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EntityCacheKey that = (EntityCacheKey) o;
    return Objects.equals(dataKey, that.dataKey) &&
        Objects.equals(tenantId, that.tenantId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataKey, tenantId);
  }
}
