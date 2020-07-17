package org.hypertrace.entity.data.service.client;

import java.util.Objects;

public class EdsCacheKey {

  public final String tenantId;
  public final String entityId;

  public EdsCacheKey(String tenantId, String entityId) {
    this.tenantId = tenantId;
    this.entityId = entityId;
  }

  @Override
  public String toString() {
    return String.format("__%s__%s", this.tenantId, this.entityId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, entityId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EdsCacheKey that = (EdsCacheKey) o;
    return Objects.equals(tenantId, that.tenantId)
        && Objects.equals(entityId, that.entityId);
  }
}
