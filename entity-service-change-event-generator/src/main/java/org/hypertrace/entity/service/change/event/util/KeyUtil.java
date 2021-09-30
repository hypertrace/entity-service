package org.hypertrace.entity.service.change.event.util;

import org.hypertrace.entity.data.service.v1.Entity;

public class KeyUtil {

  private KeyUtil() {}

  public static final String getKey(Entity entity) {
    return String.format(
        "%s:%s:%s", entity.getTenantId(), entity.getEntityType(), entity.getEntityId());
  }
}
