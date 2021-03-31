package org.hypertrace.entity.data.service;

import java.util.Objects;
import org.hypertrace.core.documentstore.Key;

class EntityV2TypeDocKey implements Key {
  private final String tenantId;
  private final String entityType;
  private final String entityId;

  EntityV2TypeDocKey(String tenantId, String entityType, String entityId) {
    this.tenantId = tenantId;
    this.entityType = entityType;
    this.entityId = entityId;
  }

  @Override
  public String toString() {
    return String.format("%s:%s:%s", this.tenantId, this.entityType, this.entityId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EntityV2TypeDocKey that = (EntityV2TypeDocKey) o;
    return Objects.equals(tenantId, that.tenantId)
        && Objects.equals(entityType, that.entityType)
        && Objects.equals(entityId, that.entityId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, entityType, entityId);
  }
}
