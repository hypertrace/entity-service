package org.hypertrace.entity.service.change.event.util;

import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.data.service.v1.Entity;

public class KeyUtil {

  private KeyUtil() {}

  public static final EntityChangeEventKey getKey(Entity entity) {
    return EntityChangeEventKey.newBuilder()
        .setTenantId(entity.getTenantId())
        .setEntityType(entity.getEntityType())
        .setEntityId(entity.getEntityId())
        .build();
  }
}
