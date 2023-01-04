package org.hypertrace.entity.data.service.client;

import java.util.Objects;

public class EdsCacheKey {

  private static final String EMPTY = "";
  public final String tenantId;
  public final String entityId;
  public final String entityType;

  public EdsCacheKey(String tenantId, String entityId) {
    this(tenantId, entityId, EMPTY);
  }

  public EdsCacheKey(String tenantId, String entityId, String entityType) {
    this.tenantId = tenantId;
    this.entityId = entityId;
    this.entityType = entityType;
  }

  @Override
  public String toString() {
    return String.format("__%s__%s__%s", this.tenantId, this.entityId, this.entityType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, entityId, entityType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EdsCacheKey that = (EdsCacheKey) o;
    return Objects.equals(tenantId, that.tenantId)
        && Objects.equals(entityId, that.entityId)
        && Objects.equals(entityType, that.entityType);
  }
}
