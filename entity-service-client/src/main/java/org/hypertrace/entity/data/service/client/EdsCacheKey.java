package org.hypertrace.entity.data.service.client;

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
}
